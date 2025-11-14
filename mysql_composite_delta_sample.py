"""
Curated sample of the MySQL composite delta extraction workflow.

This file captures the code I authored inside `mysql_extraction.py` and
adds commentary that explains how each piece participates in building and
executing the composite delta query used by the extraction runtime.
"""

from dataclasses import dataclass
from typing import List
import hashlib
import os
import pymysql.cursors
import traceback


@dataclass
class CompositeJoinCondition:
    """Declarative mapping between a root table column and a join column."""
    RootColumn: str
    JoinColumn: str


@dataclass
class CompositeJoinTable:
    """Represents one table that must be joined into the composite query."""
    JoinSchema: str
    JoinTable: str
    JoinConditions: List[CompositeJoinCondition]


@dataclass
class CompositeDeltaColumn:
    """
    Identifies the schema.table.column tuple that should be used as a
    delta key (typically a timestamp).
    """
    Schema: str
    Table: str
    Column: str


@dataclass
class CompositeDeltaConfig:
    """
    Minimal configuration object that mirrors the runtime configuration
    used by the extraction service.
    """
    RootSchema: str
    RootTable: str
    JoinTables: List[CompositeJoinTable]
    DeltaColumns: List[CompositeDeltaColumn]


class SampleCompositeDeltaExtraction:
    """
    Lightweight version of the production MySQL extraction that focuses on
    the composite delta logic. The rest of the extraction interface has
    been trimmed for readability since it is not needed for this sample.
    """

    def __init__(self, host, user, password, database, port, log):
        self._Host = host
        self._UserName = user
        self._Password = password
        self._DB = database
        self._Port = port
        self._Log = log

    def generate_composite_delta_extract(
        self,
        composite_delta_config: CompositeDeltaConfig,
        target_dir,
        batch_size,
        start_date,
        end_date,
        field_delimiter,
        record_delimiter,
        pci_columns,
        pci_salt,
    ):
        """
        Build and execute a composite delta query for a MySQL source.

        The method expands the join graph defined in `composite_delta_config`,
        constrains the results by the provided date range, and streams the
        results to disk in batches so the memory footprint remains small.
        """
        try:
            columns = self._get_columns_for_table_and_schema(
                composite_delta_config.RootSchema,
                composite_delta_config.RootTable,
            )
            pci_ordinals = self.get_pci_ordinals_from_column_names(columns, pci_columns)
            columns = [f"root.`{column}`" for column in columns]

            # 1) Build the dynamic join map defined in the configuration.
            query_template = (
                "SELECT {columns} FROM `{root_schema}`.`{root_table}` root {joins} "
                "WHERE {key_conditions};"
            )
            joins = []
            for join_table in composite_delta_config.JoinTables:
                alias = f"{join_table.JoinSchema}{join_table.JoinTable}"
                conditions = [
                    "root.`{root_column}` = {alias}.`{join_column}`".format(
                        root_column=condition.RootColumn,
                        alias=alias,
                        join_column=condition.JoinColumn,
                    )
                    for condition in join_table.JoinConditions
                ]
                joins.append(
                    "JOIN `{join_schema}`.`{join_table}` {alias} ON {conditions}".format(
                        join_schema=join_table.JoinSchema,
                        join_table=join_table.JoinTable,
                        alias=alias,
                        conditions=" AND ".join(conditions),
                    )
                )

            # 2) Derive the delta filter clause; each key is OR'd together.
            where_template = (
                "(CAST({key} AS DATE) > '{min_dt}' AND CAST({key} AS DATE) <= '{max_dt}')"
            )
            where_conditions = [
                where_template.format(
                    key=f"{delta.Schema}{delta.Table}.{delta.Column}",
                    min_dt=start_date,
                    max_dt=end_date,
                )
                for delta in composite_delta_config.DeltaColumns
            ]

            query = query_template.format(
                columns=", ".join(columns),
                root_schema=composite_delta_config.RootSchema,
                root_table=composite_delta_config.RootTable,
                joins=" ".join(joins),
                key_conditions=" OR ".join(where_conditions),
            )

            self._Log.debug(query)

            # 3) Execute the query and write the results in batches.
            connection = self._open_connection()
            cursor = self._get_dict_cursor(connection)
            cursor.execute(query)
            extract_file_path = os.path.join(
                target_dir,
                f"EXTRACT.{self._DB.upper()}__"
                f"{composite_delta_config.RootSchema.upper()}__"
                f"{composite_delta_config.RootTable.upper()}",
            )
            with open(extract_file_path, "w+") as extract_file:
                results = cursor.fetchmany(batch_size)
                while results:
                    self.write_batch_to_file(
                        extract_file,
                        results,
                        field_delimiter,
                        record_delimiter,
                        pci_ordinals,
                        pci_salt,
                    )
                    results = cursor.fetchmany(batch_size)

            self._close_db_cursor(cursor)
            self._close_db_connection(connection)
            return True, extract_file_path
        except Exception as ex:
            self._Log.critical(
                f"MySQLExtraction: Failed to create composite delta extract with error: {ex}"
            )
            self._Log.critical(traceback.format_exc())
            return False, (
                "MySQLExtraction: Failed to create composite delta extract with error: "
                f"{ex}"
            )

    #
    # -------- Helper methods that keep the composite delta workflow readable --------
    #
    def _open_connection(self):
        """Encapsulates the MySQL connection creation so retry logic can be centralized."""
        return pymysql.connect(
            host=self._Host,
            user=self._UserName,
            password=self._Password,
            database=self._DB,
            port=self._Port,
            autocommit=True,
        )

    def _get_dict_cursor(self, connection):
        """Always use a DictCursor so column order aligns with the metadata lookup."""
        return connection.cursor(pymysql.cursors.DictCursor)

    def _close_db_connection(self, connection):
        connection.close()
        self._Log.debug("Disconnected from MySQL Database")

    def _close_db_cursor(self, cursor):
        cursor.close()
        self._Log.debug("MYSQL DB cursor closed.")

    def _get_columns_for_table_and_schema(self, table_schema, table_name):
        """
        Query INFORMATION_SCHEMA to derive the ordered column list.
        This allows the extraction to dynamically support any MySQL table
        without keeping schema metadata in application code.
        """
        query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE
                table_name = '{table_name}'
                AND  table_schema = '{table_schema}'
            ORDER BY
                ordinal_position
            ;
        """
        connection = self._open_connection()
        cursor = self._get_dict_cursor(connection)
        cursor.execute(query)
        results = cursor.fetchall()
        self._close_db_cursor(cursor)
        self._close_db_connection(connection)
        return [column["COLUMN_NAME"] for column in results]

    def write_batch_to_file(
        self,
        file_handler,
        values,
        field_delimiter,
        record_delimiter,
        pci_ordinals,
        pci_salt,
    ):
        """
        Stream the current batch to disk while hashing the configured PCI columns.
        The hashing mirrors the base extraction implementation so that sensitive
        values never exist in plaintext outside of the process.
        """
        for row in values:
            record = [str(value) if value is not None else "" for value in row.values()]
            for ordinal in pci_ordinals:
                record[ordinal] = self.hash_value(record[ordinal], pci_salt)

            file_handler.write(
                field_delimiter.join(record) + record_delimiter
            )

    @staticmethod
    def hash_value(value, salt):
        return hashlib.sha512((str(value) + salt).encode()).hexdigest()

    @staticmethod
    def get_pci_ordinals_from_column_names(column_names, pci_columns):
        """Return the zero-based ordinals for every PCI column in the select list."""
        return [column_names.index(pci_column) for pci_column in pci_columns]

