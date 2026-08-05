# import requests
import re
from datetime import datetime
from time import sleep
from typing import Literal

import pandas as pd
import numpy as np
import yaml


#init_check checked of de check table al bestaat. Zo nee, dan maakt hij alle benodigde tabellen aan, het schema, etc.

class sqlConnection():

    def __init__(self, configSql, sqlUser=None, sqlPassword=None, serverLoginType: Literal["credentials", "entraId"] = "credentials"):
        self.serverLoginType = serverLoginType
        self.connectionString = self.constructConstring(configSql, sqlUser, sqlPassword, serverLoginType)
        self.open = None
        self.connection = self.openConnection()
        self.cursorSingle = self.get_cursor()
        self.cursorMany = self.get_cursor(fast=True)
        self.configSql = configSql

    def __str__(self):
        return f"SQLConnection(server='{self.configSql['server']}', database='{self.configSql['database']}', open={'True' if self.open else 'False'})"

    def constructConstring(self, sqlConnection, sqlUser, sqlPassword, serverLoginType: Literal["credentials", "entraId"] = "credentials"):
        c = sqlConnection
        if serverLoginType == "entraId":
            authSegment = "Authentication=ActiveDirectoryDefault;"
        elif c['local']:
            authSegment = "Trusted_Connection=yes;"
        else:
            authSegment = f"Uid={sqlUser};PWD={sqlPassword};"

        return (
            f"""Driver={{ODBC Driver 18 for SQL Server}};"""
            f"""Server={c["server"]}{','+str(c['port']) if not c['local'] else ''};"""
            f"""Database={c["database"]};"""
            f"{authSegment}"
            f"""Encrypt=yes;"""
            f"""TrustServerCertificate={'yes' if c.get('trust_server_certificate', False) else 'no'};"""
            f"""Connection Timeout=30;"""
        )
    def openConnection(self):
        try:
            import pyodbc
        except ImportError:
            raise ImportError("pyodbc is required. Install with pip install pyodbc")

        for i in range(5):
            try:
                self.open = True
                return pyodbc.connect(self.connectionString)
            except Exception as e:
                print(f"Error in sqlConnection >>>> {e}")
                if i == 4:
                    self.open = False
                    print(f"Fout bij het openen van de verbinding: {e}")
                    raise e
                wait = 2 ** i+1  # 1s, 2s, 4s
                print(f"Wachten {wait} seconden voor volgende poging...")
                sleep(wait)


    def get_cursor(self, fast=False):
        cur = self.connection.cursor()
        if fast:
            cur.fast_executemany = True
        return cur

    def _ensure_connection(self):
        try:
            self.cursorSingle.execute("SELECT 1")
            self.cursorSingle.fetchone()
        except Exception:
            print("Herstellen verbinding met SQL server")
            self.connection = self.openConnection()
            self.cursorSingle = self.get_cursor()
            self.cursorMany = self.get_cursor(fast=True)


    def execSingle(self, query, params=None, return_as: Literal["dict", "df", "list", "single"] | None = None, returnColumns=False):
            self._ensure_connection()

            self.cursorSingle.execute(query, params or ())
            if self.cursorSingle.description is not None:
                rows = self.cursorSingle.fetchall()
                columns = [desc[0] for desc in self.cursorSingle.description]
                self.connection.commit()

                if return_as:
                    if return_as == "dict":
                        return [dict(zip(columns, row)) for row in rows]
                    elif return_as == "df":
                        return pd.DataFrame([tuple(r) for r in rows], columns=columns)
                    elif return_as == "list":
                        return rows
                    elif return_as == "single":
                        return rows[0][0] if rows else None
                elif returnColumns:
                    return columns, rows
                else:
                    return rows
            else:
                self.connection.commit()
                return None

    @staticmethod
    def _quoteIdent(name: str) -> str:
        """Valideer en quote een SQL Server identifier (schema/tabel/kolom)."""
        if not isinstance(name, str) or not name:
            raise ValueError(f"Invalid identifier: {name!r}")
        return "[" + name.replace("]", "]]") + "]"

    def writeMany(self, df: pd.DataFrame, tableName, schemaName, merge=True, mergeKeys: list = None, syncTableName='syncLog', log=True, logTableName=None, updateInsertedTs=False):
        self._ensure_connection()
        schema = self._quoteIdent(schemaName)
        table = self._quoteIdent(tableName)

        if not df.empty:
            
            print('Found data')
            cols = df.columns.to_list()
            df[df.select_dtypes(include="bool").columns] = df.select_dtypes(include="bool").astype(int)
            df = df.where(pd.notna(df), None)

            qCols = {c: self._quoteIdent(c) for c in cols}

            if merge and not mergeKeys:
                raise Exception("mergeKeys is required when merge is True")
            
            if not merge:
                placeholders = ", ".join("?" for _ in cols)
                self.execSingle(f"TRUNCATE TABLE {schema}.{table}")
                self.cursorMany.executemany(f"INSERT INTO {schema}.{table} ({', '.join(qCols[c] for c in cols)}) VALUES({placeholders})", self.parseValues(df))
            else:
                
                query = f"""
                    MERGE INTO {schema}.{table} AS T
                    USING (SELECT {', '.join(f"? AS {qCols[c]}" for c in cols)}) AS S
                    ON {" AND ".join(f"T.{qCols[c]} = S.{qCols[c]}" for c in mergeKeys)}
                    WHEN MATCHED THEN
                        UPDATE SET
                        {', '.join(f"T.{qCols[c]} = S.{qCols[c]}" for c in [c for c in cols if c not in mergeKeys])}
                        {f", T.insertedTimestamp = SYSDATETIMEOFFSET()" if updateInsertedTs else ""}
                    WHEN NOT MATCHED THEN
                        INSERT ({', '.join(qCols[c] for c in cols)})
                        VALUES ({', '.join(f"S.{qCols[c]}" for c in cols)});
                """
                rows = self.parseValues(df)
                for i in range(0, len(rows), 10000):
                    self.cursorMany.executemany(query, rows[i:i+10000])
                    self.connection.commit()
        print("Committing...")
        if log:
            self.execSingle(
                f"""
                    INSERT INTO {schema}.{self._quoteIdent(syncTableName)}
                        (tableName, date, records, insertedTimestamp)
                    VALUES (?, ?, ?, SYSDATETIMEOFFSET());
                """,
                params=[logTableName or tableName, datetime.now().date(), len(df)]

                )

        self.connection.commit()
        return

    def writeManyEntity(self, df: pd.DataFrame, tableName, schemaName, entity, merge=True, mergeKeys: list = None, syncTableName='syncLog', log=True, logTableName=None, entityColName='entity', updateInsertedTs=False):
        self._ensure_connection()
        schema = self._quoteIdent(schemaName)
        table = self._quoteIdent(tableName)

        if not df.empty:

            print('Found data')
            cols = df.columns.to_list()
            df[df.select_dtypes(include="bool").columns] = df.select_dtypes(include="bool").astype(int)
            df = df.where(pd.notna(df), None)

            qCols = {c: self._quoteIdent(c) for c in cols}

            if merge and not mergeKeys:
                raise Exception("mergeKeys is required when merge is True")


            if not merge:           
                placeholders = ", ".join("?" for _ in cols)
                self.execSingle(f"DELETE FROM {schema}.{table} WHERE {self._quoteIdent(entityColName)} = ?", params=[entity])
                self.cursorMany.executemany(f"INSERT INTO {schema}.{table} ({', '.join(qCols[c] for c in cols)}) VALUES({placeholders})", self.parseValues(df))
            else:

                query = f"""
                    MERGE INTO {schema}.{table} AS T
                    USING (SELECT {', '.join(f"? AS {qCols[c]}" for c in cols)}) AS S
                    ON {" AND ".join(f"T.{qCols[c]} = S.{qCols[c]}" for c in mergeKeys)}
                    WHEN MATCHED THEN
                        UPDATE SET
                        {', '.join(f"T.{qCols[c]} = S.{qCols[c]}" for c in [c for c in cols if c not in mergeKeys])}
                        {f", T.insertedTimestamp = SYSDATETIMEOFFSET()" if updateInsertedTs else ""}
                    WHEN NOT MATCHED THEN
                        INSERT ({', '.join(qCols[c] for c in cols)})
                        VALUES ({', '.join(f"S.{qCols[c]}" for c in cols)});
                """


                rows = self.parseValues(df)
                for i in range(0, len(rows), 10000):
                    self.cursorMany.executemany(query, rows[i:i+10000])
                    self.connection.commit()
        print("Committing...")
        if log:
            self.execSingle(
                f"""
                    INSERT INTO {schema}.{self._quoteIdent(syncTableName)}
                        (tableName, {self._quoteIdent(entityColName)}, date, records, insertedTimestamp)
                    VALUES (?, ?, ?, ?, SYSDATETIMEOFFSET());
                """,
                params=[logTableName or tableName, entity, datetime.now().date(), len(df)]
                )
        self.connection.commit()
        return

    def parseValues(self, df: pd.DataFrame):
        data = []
        for _, row in df.iterrows():
            d = []
            for col in df.columns.tolist():
                if df[col].dtype == 'object':
                    if str(row[col]) == 'None':
                        d.append(None)
                    else:
                        d.append(str(row[col]))
                else:
                    if pd.isna(row[col]) or row[col] == np.nan:
                        d.append(None)
                    else:
                        d.append(row[col])
            data.append(d)
        return data

    def close(self):
            if self.cursorSingle:
                self.cursorSingle.close()
                self.cursorSingle = None
            if self.cursorMany:
                self.cursorMany.close()
                self.cursorMany = None
            if self.connection:
                self.connection.close()
                self.connection = None
            self.open = False
            # print("Verbinding gesloten.")


    def checkExistingDatabase(self, schemaName, tableName):
        return self.execSingle(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'",
            params=[schemaName]
            )


    def constructDatabase(self, configDatabase, schemaName='dbo'):
        qSchema = self._quoteIdent(schemaName)
        existingTables = self.checkExistingDatabase(schemaName, configDatabase['tables']['monitor'][0]['name'])
        if existingTables:
            print("Database bestaat al")
            existingTables = [r[0] for r in existingTables]
        else:
            print("Database bestaat nog niet. Maak aan...")
            querySchema = "IF SCHEMA_ID(?) IS NULL BEGIN EXEC('CREATE SCHEMA ' + ?) END"
            self.execSingle(querySchema, params=[schemaName, qSchema])
            existingTables = []

        monitor = configDatabase['tables']['monitor']
        fact = configDatabase['tables']['fact']
        meta = configDatabase['tables']['meta']

        tables = (monitor or []) + (meta or []) + (fact or [])
        tables = [t for t in tables if t['name'] not in existingTables]

        for table in tables:
            qTable = self._quoteIdent(table['name'])
            constraints = []
            for constraint in table.get('constraints', []):
                qConstraintName = self._quoteIdent(f"{table['name']}_{constraint['name']}")
                qConstraintCols = ', '.join(self._quoteIdent(c) for c in constraint['columns'])
                constraints.append(f"CONSTRAINT {qConstraintName} {constraint['type']} {'CLUSTERED' if constraint['clustered'] else ''} ({qConstraintCols})")

            self.execSingle(
                f"""CREATE TABLE {qSchema}.{qTable}"""
                f"""({table['columns'].replace('$NAME', table['name'])}{(','+','.join(constraints)) if len(constraints) > 0 else ''})"""
                )

            for index in table.get('indexes', []):
                qIndexName = self._quoteIdent(f"{table['name']}_{index['name']}")
                qIndexCols = ', '.join(self._quoteIdent(c) for c in index['columns'])
                includeClause = 'INCLUDE (' + ','.join(self._quoteIdent(c) for c in index['includes']) + ')' if index.get('includes', []) else ''
                self.execSingle(f"CREATE {'CLUSTERED' if index['clustered'] else 'NONCLUSTERED'} INDEX {qIndexName} ON {qSchema}.{qTable} ({qIndexCols}) {includeClause}")

        # Foreign keys worden pas gelegd nadat alle (nieuwe) tabellen bestaan, zodat de volgorde in het template er niet toe doet.
        for table in tables:
            qTable = self._quoteIdent(table['name'])
            for fk in table.get('foreignKeys', []):
                qFkName = self._quoteIdent(f"{table['name']}_{fk['name']}")
                qFkCols = ', '.join(self._quoteIdent(c) for c in fk['columns'])
                qRefSchema = self._quoteIdent(fk.get('refSchema', schemaName))
                qRefTable = self._quoteIdent(fk['refTable'])
                qRefCols = ', '.join(self._quoteIdent(c) for c in fk['refColumns'])
                onDelete = f" ON DELETE {fk['onDelete']}" if fk.get('onDelete') else ''
                onUpdate = f" ON UPDATE {fk['onUpdate']}" if fk.get('onUpdate') else ''
                self.execSingle(
                    f"ALTER TABLE {qSchema}.{qTable} ADD CONSTRAINT {qFkName} "
                    f"FOREIGN KEY ({qFkCols}) REFERENCES {qRefSchema}.{qRefTable} ({qRefCols})"
                    f"{onDelete}{onUpdate}"
                )


class postgresConnection():

    def __init__(self, configSql, sqlUser=None, sqlPassword=None):
        self.configSql = configSql
        self.connectionParams = self.constructConnParams(configSql, sqlUser, sqlPassword)
        self.open = None
        self.connection = self.openConnection()
        self.cursorSingle = self.get_cursor()
        self.cursorMany = self.get_cursor(fast=True)

    def __str__(self):
        return f"postgresConnection(server='{self.configSql['server']}', database='{self.configSql['database']}', open={'True' if self.open else 'False'})"

    def constructConnParams(self, configSql, sqlUser=None, sqlPassword=None):
        c = configSql
        configType = c.get("configType", "local-dev")

        params = {
            "host": "localhost" if configType == "server-deployed" else c["server"],
            "port": c.get("port", 5432),
            "dbname": c["database"],
            "connect_timeout": 30,
        }

        if configType == "server":
            params["sslmode"] = "require"

        if sqlUser:
            params["user"] = sqlUser
        if sqlPassword:
            params["password"] = sqlPassword

        return params

    def openConnection(self):
        try:
            import psycopg2
        except ImportError:
            raise ImportError("psycopg2 is required. Install with pip install psycopg2-binary")

        for i in range(5):
            try:
                self.open = True
                return psycopg2.connect(**self.connectionParams)
            except Exception as e:
                print(f"Error in postgresConnection >>>> {e}")
                if i == 4:
                    self.open = False
                    print(f"Fout bij het openen van de verbinding: {e}")
                    raise e
                wait = 2 ** i+1  # 1s, 2s, 4s
                print(f"Wachten {wait} seconden voor volgende poging...")
                sleep(wait)

    def get_cursor(self, fast=False):
        return self.connection.cursor()

    def _ensure_connection(self):
        try:
            self.cursorSingle.execute("SELECT 1")
            self.cursorSingle.fetchone()
        except Exception:
            print("Herstellen verbinding met Postgres server")
            self.connection = self.openConnection()
            self.cursorSingle = self.get_cursor()
            self.cursorMany = self.get_cursor(fast=True)

    def execSingle(self, query, params=None, return_as: Literal["dict", "df", "list", "single"] | None = None, returnColumns=False):
            self._ensure_connection()

            self.cursorSingle.execute(query, params or ())
            if self.cursorSingle.description is not None:
                rows = self.cursorSingle.fetchall()
                columns = [desc[0] for desc in self.cursorSingle.description]
                self.connection.commit()

                if return_as:
                    if return_as == "dict":
                        return [dict(zip(columns, row)) for row in rows]
                    elif return_as == "df":
                        return pd.DataFrame([tuple(r) for r in rows], columns=columns)
                    elif return_as == "list":
                        return rows
                    elif return_as == "single":
                        return rows[0][0] if rows else None
                elif returnColumns:
                    return columns, rows
                else:
                    return rows
            else:
                self.connection.commit()
                return None

    @staticmethod
    def _quoteIdent(name: str) -> str:
        """Valideer en quote een Postgres identifier (schema/tabel/kolom)."""
        if not isinstance(name, str) or not name:
            raise ValueError(f"Invalid identifier: {name!r}")
        return '"' + name.replace('"', '""') + '"'

    @staticmethod
    def _splitTopLevel(text: str) -> list:
        parts = []
        depth = 0
        current = []
        for ch in text:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            if ch == ',' and depth == 0:
                parts.append(''.join(current))
                current = []
            else:
                current.append(ch)
        parts.append(''.join(current))
        return parts

    def _quoteColumnDefs(self, rawColumns: str) -> str:
        """Quote de kolomnaam vooraan elke top-level CREATE TABLE-segment, zodat camelCase
        behouden blijft in Postgres (dat unquoted identifiers naar lowercase vouwt). Segmenten
        die beginnen met CONSTRAINT (table-level constraints) worden ongemoeid gelaten."""
        segments = self._splitTopLevel(rawColumns)
        quoted = []
        for segment in segments:
            stripped = segment.strip()
            if not stripped or stripped.upper().startswith("CONSTRAINT"):
                quoted.append(segment)
                continue
            match = re.match(r'^(\s*)(\[[^\]]+\]|"[^"]+"|\S+)(.*)$', segment, re.DOTALL)
            if not match:
                quoted.append(segment)
                continue
            leading, ident, rest = match.groups()
            ident = ident.strip('[]"')
            quoted.append(f"{leading}{self._quoteIdent(ident)}{rest}")
        return ','.join(quoted)

    def writeMany(self, df: pd.DataFrame, tableName, schemaName, merge=True, mergeKeys: list = None, syncTableName='syncLog', log=True, logTableName=None, updateInsertedTs=False):
        from psycopg2.extras import execute_values

        self._ensure_connection()
        schema = self._quoteIdent(schemaName)
        table = self._quoteIdent(tableName)

        if not df.empty:

            print('Found data')
            cols = df.columns.to_list()
            boolCols = df.select_dtypes(include="bool").columns
            df[boolCols] = df[boolCols].astype(object)  # numpy.bool_ -> Python bool, want psycopg2 kan geen int in een boolean-kolom schrijven
            df = df.where(pd.notna(df), None)

            qCols = {c: self._quoteIdent(c) for c in cols}

            if merge and not mergeKeys:
                raise Exception("mergeKeys is required when merge is True")

            rows = [tuple(r) for r in self.parseValues(df)]

            if not merge:
                self.execSingle(f"TRUNCATE TABLE {schema}.{table}")
                execute_values(
                    self.cursorMany,
                    f"INSERT INTO {schema}.{table} ({', '.join(qCols[c] for c in cols)}) VALUES %s",
                    rows,
                    page_size=10000,
                )
            else:
                # ON CONFLICT vereist een unique/primary key constraint op mergeKeys in de doeltabel
                updateCols = [c for c in cols if c not in mergeKeys]
                setClause = ', '.join(f"{qCols[c]} = EXCLUDED.{qCols[c]}" for c in updateCols)
                if updateInsertedTs:
                    setClause += ", insertedTimestamp = now()"
                query = (
                    f"INSERT INTO {schema}.{table} ({', '.join(qCols[c] for c in cols)}) VALUES %s "
                    f"ON CONFLICT ({', '.join(qCols[c] for c in mergeKeys)}) DO UPDATE SET {setClause}"
                )
                execute_values(self.cursorMany, query, rows, page_size=10000)
            self.connection.commit()
        print("Committing...")
        if log:
            self.execSingle(
                f"""
                    INSERT INTO {schema}.{self._quoteIdent(syncTableName)}
                        ({self._quoteIdent('tableName')}, date, records, {self._quoteIdent('insertedTimestamp')})
                    VALUES (%s, %s, %s, now());
                """,
                params=[logTableName or tableName, datetime.now().date(), len(df)]

                )

        self.connection.commit()
        return

    def writeManyEntity(self, df: pd.DataFrame, tableName, schemaName, entity, merge=True, mergeKeys: list = None, syncTableName='syncLog', log=True, logTableName=None, entityColName='entity', updateInsertedTs=False):
        from psycopg2.extras import execute_values

        self._ensure_connection()
        schema = self._quoteIdent(schemaName)
        table = self._quoteIdent(tableName)

        if not df.empty:

            print('Found data')
            cols = df.columns.to_list()
            boolCols = df.select_dtypes(include="bool").columns
            df[boolCols] = df[boolCols].astype(object)  # numpy.bool_ -> Python bool, want psycopg2 kan geen int in een boolean-kolom schrijven
            df = df.where(pd.notna(df), None)

            qCols = {c: self._quoteIdent(c) for c in cols}

            if merge and not mergeKeys:
                raise Exception("mergeKeys is required when merge is True")

            rows = [tuple(r) for r in self.parseValues(df)]

            if not merge:
                self.execSingle(f"DELETE FROM {schema}.{table} WHERE {self._quoteIdent(entityColName)} = %s", params=[entity])
                execute_values(
                    self.cursorMany,
                    f"INSERT INTO {schema}.{table} ({', '.join(qCols[c] for c in cols)}) VALUES %s",
                    rows,
                    page_size=10000,
                )
            else:
                # ON CONFLICT vereist een unique/primary key constraint op mergeKeys in de doeltabel
                updateCols = [c for c in cols if c not in mergeKeys]
                setClause = ', '.join(f"{qCols[c]} = EXCLUDED.{qCols[c]}" for c in updateCols)
                if updateInsertedTs:
                    setClause += ", insertedTimestamp = now()"
                query = (
                    f"INSERT INTO {schema}.{table} ({', '.join(qCols[c] for c in cols)}) VALUES %s "
                    f"ON CONFLICT ({', '.join(qCols[c] for c in mergeKeys)}) DO UPDATE SET {setClause}"
                )
                execute_values(self.cursorMany, query, rows, page_size=10000)
            self.connection.commit()
        print("Committing...")
        if log:
            self.execSingle(
                f"""
                    INSERT INTO {schema}.{self._quoteIdent(syncTableName)}
                        ({self._quoteIdent('tableName')}, {self._quoteIdent(entityColName)}, date, records, {self._quoteIdent('insertedTimestamp')})
                    VALUES (%s, %s, %s, %s, now());
                """,
                params=[logTableName or tableName, entity, datetime.now().date(), len(df)]
                )
        self.connection.commit()
        return

    def parseValues(self, df: pd.DataFrame):
        data = []
        for _, row in df.iterrows():
            d = []
            for col in df.columns.tolist():
                value = row[col]
                if isinstance(value, (bool, np.bool_)):
                    # eigen tak nodig: anders komt een bool via de 'object'-tak als de tekst 'True'/'False'
                    # binnen, en dat kan psycopg2 niet naar een boolean-kolom schrijven
                    d.append(bool(value) if pd.notna(value) else None)
                elif df[col].dtype == 'object':
                    if str(value) == 'None':
                        d.append(None)
                    else:
                        d.append(str(value))
                else:
                    if pd.isna(value) or value == np.nan:
                        d.append(None)
                    else:
                        d.append(value)
            data.append(d)
        return data

    def close(self):
            if self.cursorSingle:
                self.cursorSingle.close()
                self.cursorSingle = None
            if self.cursorMany:
                self.cursorMany.close()
                self.cursorMany = None
            if self.connection:
                self.connection.close()
                self.connection = None
            self.open = False
            # print("Verbinding gesloten.")

    def checkExistingDatabase(self, schemaName, tableName):
        return self.execSingle(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type = 'BASE TABLE'",
            params=[schemaName]
            )

    def constructDatabase(self, configDatabase, schemaName='dbo'):
        qSchema = self._quoteIdent(schemaName)
        existingTables = self.checkExistingDatabase(schemaName, configDatabase['tables']['monitor'][0]['name'])
        if existingTables:
            print("Database bestaat al")
            existingTables = [r[0] for r in existingTables]
        else:
            print("Database bestaat nog niet. Maak aan...")
            self.execSingle(f"CREATE SCHEMA IF NOT EXISTS {qSchema}")
            existingTables = []

        monitor = configDatabase['tables']['monitor']
        fact = configDatabase['tables']['fact']
        meta = configDatabase['tables']['meta']

        tables = (monitor or []) + (meta or []) + (fact or [])
        tables = [t for t in tables if t['name'] not in existingTables]

        for table in tables:
            qTable = self._quoteIdent(table['name'])
            constraints = []
            for constraint in table.get('constraints', []):
                qConstraintName = self._quoteIdent(f"{table['name']}_{constraint['name']}")
                qConstraintCols = ', '.join(self._quoteIdent(c) for c in constraint['columns'])
                constraints.append(f"CONSTRAINT {qConstraintName} {constraint['type']} ({qConstraintCols})")

            columnDefs = self._quoteColumnDefs(table['columns'].replace('$NAME', table['name']))
            self.execSingle(
                f"""CREATE TABLE {qSchema}.{qTable}"""
                f"""({columnDefs}{(','+','.join(constraints)) if len(constraints) > 0 else ''})"""
                )

            for index in table.get('indexes', []):
                qIndexName = self._quoteIdent(f"{table['name']}_{index['name']}")
                qIndexCols = ', '.join(self._quoteIdent(c) for c in index['columns'])
                includeClause = 'INCLUDE (' + ','.join(self._quoteIdent(c) for c in index['includes']) + ')' if index.get('includes', []) else ''
                self.execSingle(f"CREATE INDEX {qIndexName} ON {qSchema}.{qTable} ({qIndexCols}) {includeClause}")

        # Foreign keys worden pas gelegd nadat alle (nieuwe) tabellen bestaan, zodat de volgorde in het template er niet toe doet.
        for table in tables:
            qTable = self._quoteIdent(table['name'])
            for fk in table.get('foreignKeys', []):
                qFkName = self._quoteIdent(f"{table['name']}_{fk['name']}")
                qFkCols = ', '.join(self._quoteIdent(c) for c in fk['columns'])
                qRefSchema = self._quoteIdent(fk.get('refSchema', schemaName))
                qRefTable = self._quoteIdent(fk['refTable'])
                qRefCols = ', '.join(self._quoteIdent(c) for c in fk['refColumns'])
                onDelete = f" ON DELETE {fk['onDelete']}" if fk.get('onDelete') else ''
                onUpdate = f" ON UPDATE {fk['onUpdate']}" if fk.get('onUpdate') else ''
                self.execSingle(
                    f"ALTER TABLE {qSchema}.{qTable} ADD CONSTRAINT {qFkName} "
                    f"FOREIGN KEY ({qFkCols}) REFERENCES {qRefSchema}.{qRefTable} ({qRefCols})"
                    f"{onDelete}{onUpdate}"
                )


class secrets():
    def __init__(self, vaultUrl='', local=False, prefix='', credFolder=''):
        from azure.keyvault.secrets import SecretClient
        from azure.identity import DefaultAzureCredential

        self.local = local
        self.prefix = prefix
        self.credFolder = credFolder
        if not local:
            self.client = SecretClient(vault_url=vaultUrl, credential=DefaultAzureCredential())
        else:
            with open(f"{(credFolder+'/') if credFolder else ''}credentials.yaml", 'r') as f:
                self.creds = yaml.safe_load(f)


    def get(self, name):
        n = self.prefix+name
        if self.local:
            try:
                return self.creds[n]
            except:
                raise Exception(f"Secret {n} not found in credentials.yaml")
        else:
            try:
                return self.client.get_secret(n).value
            except:
                raise Exception(f"Secret {n} not found in keyvault")
    

    def set(self, name, value):
        n = self.prefix+name
        if self.local:
            self.creds[n] = value
            with open(f"{(self.credFolder+'/') if self.credFolder else ''}credentials.yaml", 'w') as f:
                yaml.dump(self.creds, f)
        else:
            self.client.set_secret(n, value)
