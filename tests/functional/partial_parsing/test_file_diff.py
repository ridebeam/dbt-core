import os
import pytest

from dbt.tests.util import run_dbt, write_artifact
from dbt.tests.fixtures.project import write_project_files


local_dependency__dbt_project_yml = """
name: 'local_dep'
version: '1.0'
config-version: 2

profile: 'default'

require-dbt-version: '>=0.1.0'

target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
    - "target"
    - "dbt_packages"

seeds:
  quote_columns: False

"""

local_dependency__models__schema_yml = """
version: 2
sources:
  - name: seed_source
    schema: schema
    tables:
      - name: "local_seed"
        columns:
          - name: id
            tests:
              - unique

seeds:
  - name: local_seed
  - name: seed
    config:
      column_types:
        id: integer
        first_name: varchar(11)
        email: varchar(31)
        ip_address: varchar(15)
        updated_at: timestamp without time zone

"""

local_dependency__models__model_to_import_sql = """
select * from {{ ref('seed') }}

"""

local_dependency__seeds__seed_csv = """id
1
"""

# This seed is to make the dbt_integration_project work, but is actually installed
# by the local_dependency
integration_project__seed_csv = """id,first_name,email,ip_address,updated_at
1,Larry,lking0@miitbeian.gov.cn,69.135.206.194,2008-09-12 19:08:31
2,Larry,lperkins1@toplist.cz,64.210.133.162,1978-05-09 04:15:14
3,Anna,amontgomery2@miitbeian.gov.cn,168.104.64.114,2011-10-16 04:07:57
4,Sandra,sgeorge3@livejournal.com,229.235.252.98,1973-07-19 10:52:43
"""


first_file_diff = {
    "deleted": [],
    "changed": [],
    "added": [{"path": "models/model_one.sql", "content": "select 1 as fun"}],
}


second_file_diff = {
    "deleted": [],
    "changed": [],
    "added": [{"path": "models/model_two.sql", "content": "select 123 as notfun"}],
}


class TestFileDiffs:
    # We need to be able to test changes to dependencies
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project_root):
        local_dependency_files = {
            "dbt_project.yml": local_dependency__dbt_project_yml,
            "models": {
                "schema.yml": local_dependency__models__schema_yml,
                "model_to_import.sql": local_dependency__models__model_to_import_sql,
            },
            "seeds": {
                "local_seed.csv": local_dependency__seeds__seed_csv,
                "seed.csv": integration_project__seed_csv,
            },
        }
        write_project_files(project_root, "local_dependency", local_dependency_files)

    @pytest.fixture(scope="class")
    def packages(self):
        return {
            "packages": [
                {
                    "git": "https://github.com/dbt-labs/dbt-integration-project",
                    "revision": "1.1",
                },
                {"local": "local_dependency"},
            ]
        }

    #   @pytest.fixture(scope="class")
    #   def project_config_update(self):
    #       return {
    #           "seeds": {
    #               "quote_columns": False,
    #               "test": {
    #                   "seed": {
    #                       "+column_types": {
    #                           "id": "integer",
    #                           "first_name": "varchar(11)",
    #                           "email": "varchar(31)",
    #                           "ip_address": "varchar(15)",
    #                           "updated_at": "timestamp without time zone",
    #                       },
    #                   },
    #               },
    #           },
    #       }

    def test_file_diffs(self, project):

        os.environ["DBT_PP_FILE_DIFF_TEST"] = "true"

        run_dbt(["deps"])
        run_dbt(["seed"])

        # We start with an empty project
        results = run_dbt()

        write_artifact(first_file_diff, "file_diff.json")
        results = run_dbt()
        assert len(results) == 5

        write_artifact(second_file_diff, "file_diff.json")
        results = run_dbt()
        assert len(results) == 6
