import os
import pathspec  # type: ignore
import pathlib
from dataclasses import dataclass, field
from dbt.clients.system import load_file_contents
from dbt.contracts.files import (
    FilePath,
    ParseFileType,
    SourceFile,
    FileHash,
    AnySourceFile,
    SchemaSourceFile,
)
from dbt.config import Project
from dbt.parser.schemas import yaml_from_file, schema_file_keys, check_format_version
from dbt.exceptions import ParsingError
from dbt.parser.search import filesystem_search
from typing import Optional, Dict, List, Mapping


@dataclass
class InputFile:
    path: str
    contents: str


@dataclass
class ProjectFileDiff:
    project_name: str
    deleted_files: List[str]
    changed_files: List


@dataclass
class FileDiff:
    projects: List[ProjectFileDiff]


# This loads the files contents and creates the SourceFile object
def load_source_file(
    path: FilePath,
    parse_file_type: ParseFileType,
    project_name: str,
    saved_files,
) -> Optional[AnySourceFile]:

    sf_cls = SchemaSourceFile if parse_file_type == ParseFileType.Schema else SourceFile
    source_file = sf_cls(
        path=path,
        checksum=FileHash.empty(),
        parse_file_type=parse_file_type,
        project_name=project_name,
    )

    skip_loading_schema_file = False
    if (
        parse_file_type == ParseFileType.Schema
        and saved_files
        and source_file.file_id in saved_files
    ):
        old_source_file = saved_files[source_file.file_id]
        if (
            source_file.path.modification_time != 0.0
            and old_source_file.path.modification_time == source_file.path.modification_time
        ):
            source_file.checksum = old_source_file.checksum
            source_file.dfy = old_source_file.dfy
            skip_loading_schema_file = True

    if not skip_loading_schema_file:
        file_contents = load_file_contents(path.absolute_path, strip=False)
        source_file.checksum = FileHash.from_contents(file_contents)
        source_file.contents = file_contents.strip()

    if parse_file_type == ParseFileType.Schema and source_file.contents:
        dfy = yaml_from_file(source_file)
        if dfy:
            validate_yaml(source_file.path.original_file_path, dfy)
            source_file.dfy = dfy
        else:
            source_file = None
    return source_file


# Do some minimal validation of the yaml in a schema file.
# Check version, that key values are lists and that each element in
# the lists has a 'name' key
def validate_yaml(file_path, dct):
    check_format_version(file_path, dct)
    for key in schema_file_keys:
        if key in dct:
            if not isinstance(dct[key], list):
                msg = (
                    f"The schema file at {file_path} is "
                    f"invalid because the value of '{key}' is not a list"
                )
                raise ParsingError(msg)
            for element in dct[key]:
                if not isinstance(element, dict):
                    msg = (
                        f"The schema file at {file_path} is "
                        f"invalid because a list element for '{key}' is not a dictionary"
                    )
                    raise ParsingError(msg)
                if "name" not in element:
                    msg = (
                        f"The schema file at {file_path} is "
                        f"invalid because a list element for '{key}' does not have a "
                        "name attribute."
                    )
                    raise ParsingError(msg)


# Special processing for big seed files
def load_seed_source_file(match: FilePath, project_name) -> SourceFile:
    if match.seed_too_large():
        # We don't want to calculate a hash of this file. Use the path.
        source_file = SourceFile.big_seed(match)
    else:
        file_contents = load_file_contents(match.absolute_path, strip=False)
        checksum = FileHash.from_contents(file_contents)
        source_file = SourceFile(path=match, checksum=checksum)
        source_file.contents = ""
    source_file.parse_file_type = ParseFileType.Seed
    source_file.project_name = project_name
    return source_file


# Use the FilesystemSearcher to get a bunch of FilePaths, then turn
# them into a bunch of FileSource objects
def get_source_files(project, paths, extension, parse_file_type, saved_files, ignore_spec):
    # file path list
    fp_list = filesystem_search(project, paths, extension, ignore_spec)
    # file block list
    fb_list = []
    for fp in fp_list:
        if parse_file_type == ParseFileType.Seed:
            fb_list.append(load_seed_source_file(fp, project.project_name))
        # singular tests live in /tests but only generic tests live
        # in /tests/generic so we want to skip those
        else:
            if parse_file_type == ParseFileType.SingularTest:
                path = pathlib.Path(fp.relative_path)
                if path.parts[0] == "generic":
                    continue
            file = load_source_file(fp, parse_file_type, project.project_name, saved_files)
            # only append the list if it has contents. added to fix #3568
            if file:
                fb_list.append(file)
    return fb_list


def read_files_for_parser(project, files, parse_ft, file_type_info, saved_files, ignore_spec):
    dirs = file_type_info["paths"]
    parser_files = []
    for extension in file_type_info["extensions"]:
        source_files = get_source_files(
            project, dirs, extension, parse_ft, saved_files, ignore_spec
        )
        for sf in source_files:
            files[sf.file_id] = sf
            parser_files.append(sf.file_id)
    return parser_files


def generate_dbt_ignore_spec(project_root):
    ignore_file_path = os.path.join(project_root, ".dbtignore")

    ignore_spec = None
    if os.path.exists(ignore_file_path):
        with open(ignore_file_path) as f:
            ignore_spec = pathspec.PathSpec.from_lines(pathspec.patterns.GitWildMatchPattern, f)
    return ignore_spec


@dataclass
class ReadFilesFromFileSystem:
    all_projects: Mapping[str, Project]
    # This is a reference to the "files" dictionary in the current manifest, so the
    # manifest in implicitly updated by this code.
    files: Dict[str, AnySourceFile]
    # saved_files is only used to compare schema files
    saved_files: Dict[str, AnySourceFile] = field(default_factory=dict)
    # project_parser_files = {
    #   "my_project": {
    #     "ModelParser": ["my_project://models/my_model.sql"]
    #   }
    # }
    #
    project_parser_files: Dict = field(default_factory=dict)

    def read_files(self):
        for project in self.all_projects.values():
            file_types = get_file_types_for_project(project)
            self.read_files_for_project(project, file_types)

    def read_files_for_project(self, project, file_types):
        dbt_ignore_spec = generate_dbt_ignore_spec(project.project_root)
        project_files = self.project_parser_files[project.project_name] = {}

        for parse_ft, file_type_info in file_types.items():
            project_files[file_type_info["parser"]] = read_files_for_parser(
                project,
                self.files,
                parse_ft,
                file_type_info,
                self.saved_files,
                dbt_ignore_spec,
            )


def get_file_types_for_project(project):
    file_types = {
        ParseFileType.Macro: {
            "paths": project.macro_paths,
            "extensions": [".sql"],
            "parser": "MacroParser",
        },
        ParseFileType.Model: {
            "paths": project.model_paths,
            "extensions": [".sql", ".py"],
            "parser": "ModelParser",
        },
        ParseFileType.Snapshot: {
            "paths": project.snapshot_paths,
            "extensions": [".sql"],
            "parser": "SnapshotParser",
        },
        ParseFileType.Analysis: {
            "paths": project.analysis_paths,
            "extensions": [".sql"],
            "parser": "AnalysisParser",
        },
        ParseFileType.SingularTest: {
            "paths": project.test_paths,
            "extensions": [".sql"],
            "parser": "SingularTestParser",
        },
        ParseFileType.GenericTest: {
            "paths": project.generic_test_paths,
            "extensions": [".sql"],
            "parser": "GenericTestParser",
        },
        ParseFileType.Seed: {
            "paths": project.seed_paths,
            "extensions": [".csv"],
            "parser": "SeedParser",
        },
        ParseFileType.Documentation: {
            "paths": project.docs_paths,
            "extensions": [".md"],
            "parser": "DocumentationParser",
        },
        ParseFileType.Schema: {
            "paths": project.all_source_paths,
            "extensions": [".yml", ".yaml"],
            "parser": "SchemaParser",
        },
    }
    return file_types
