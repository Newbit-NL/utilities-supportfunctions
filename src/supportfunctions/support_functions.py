# import requests
from datetime import datetime
from time import sleep
from typing import Literal

import pandas as pd
import numpy as np
import yaml


#init_check checked of de check table al bestaat. Zo nee, dan maakt hij alle benodigde tabellen aan, het schema, etc.

class sqlConnection():

    def __init__(self, configSql, sqlUser, sqlPassword):
        self.connectionString = self.constructConstring(configSql, sqlUser, sqlPassword)
        self.open = None
        self.connection = self.openConnection()
        self.cursorSingle = self.get_cursor()
        self.cursorMany = self.get_cursor(fast=True)
        self.configSql = configSql

    def __str__(self):
        return f"SQLConnection(server='{self.configSql['server']}', database='{self.configSql['database']}', schema='{self.configSql['schema']}', open={self.open})"
    
    def constructConstring(self, sqlConnection, sqlUser, sqlPassword):
        c = sqlConnection
        return (
            f"""Driver={{ODBC Driver 18 for SQL Server}};"""
            f"""Server={c["server"]}{','+str(c['port']) if not c['local'] else ''};"""
            f"""Database={c["database"]};"""
            f"{'Uid='+sqlUser+';PWD='+sqlPassword+';' if not c['local'] else ''}"
            f"""{'Trusted_Connection=yes;' if c['local'] else ''}"""
            f"""Encrypt=yes;"""
            f"""TrustServerCertificate={'yes' if c['trust_server_certificate'] else 'no'};"""
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
                if i == 5:
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

    # def execSingle(self, query):
    #     self._ensure_connection()

    #     self.cursorSingle.execute(query)
    #     if query.strip().lower().startswith("select"):
    #         result = self.cursorSingle.fetchall()  # haalt alle rijen op
    #         return result
    #     else:
    #         self.connection.commit()
    #         return None

    def execSingle(self, query, params=None, return_as: Literal["dict", "df", "list"] | None = None, returnColumns=False):
            self._ensure_connection()

            self.cursorSingle.execute(query, params or ())
            if query.strip().lower().startswith("select"):
                rows = self.cursorSingle.fetchall()
                columns = [desc[0] for desc in self.cursorSingle.description]

                if return_as:
                    if return_as == "dict":
                        return [dict(zip(columns, row)) for row in rows]
                    elif return_as == "df":
                        return pd.DataFrame([tuple(r) for r in rows], columns=columns)
                    elif return_as == "list":
                        return rows
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

    def writeMany(self, df: pd.DataFrame, tableName, schemaName, merge=True, mergeKeys: list = None, syncTableName='syncLog', log=True, updateInsertedTs=False):
        self._ensure_connection()
        if not df.empty:
            
            print('Found data')
            cols = df.columns.to_list()
            df[df.select_dtypes(include="bool").columns] = df.select_dtypes(include="bool").astype(int)
            df = df.where(pd.notna(df), None)

            schema = self._quoteIdent(schemaName)
            table = self._quoteIdent(tableName)
            qCols = {c: self._quoteIdent(c) for c in cols}

            if merge and not mergeKeys:
                raise Exception("mergeKeys is required when merge is True")
            
            if not merge:
                placeholders = ", ".join("?" for _ in cols)
                self.execSingle(f"TRUNCATE TABLE [{schema}].[{table}]")
                self.cursorMany.executemany(f"INSERT INTO {schema}.{table} ({', '.join(qCols[c] for c in cols)}) VALUES({placeholders})", self.parseValues(df))
            else:
                
                query = f"""
                    MERGE INTO {schema}.{table} AS T
                    USING (SELECT ? AS {', ? AS '.join(f"? AS {qCols[c]}" for c in cols)}) AS S
                    ON {" AND ".join(f"T.{qCols[c]} = S.{qCols[c]}" for c in mergeKeys)}
                    WHEN MATCHED THEN
                        UPDATE SET
                        {', '.join(f"T.{qCols[c]} = S.{qCols[c]}" for c in [c for c in cols if c not in mergeKeys])}
                        {f", T.insertedTimestamp = SYSDATETIMEOFFSET()" if updateInsertedTs else ""}
                    WHEN NOT MATCHED THEN
                        INSERT ({', '.join(qCols[c] for c in cols)})
                        VALUES ({', '.join(f"S.{qCols[c]}" for c in cols)});
                """

                self.cursorMany.executemany(query, self.parseValues(df))
        print("Committing...")
        if log:
            self.execSingle(
                f"""
                    INSERT INTO {schema}.{self._quoteIdent(syncTableName)}
                        (tableName, date, records, insertedTimestamp)
                    VALUES (?, ?, ?, SYSDATETIMEOFFSET());
                """,
                params=[tableName, datetime.now().date(), len(df)]

                # f"""
                #     MERGE INTO {schema}.{syncTableName} AS target
                #     USING (VALUES('{tableName}', '{datetime.now().date()}', {len(df)}, SYSDATETIMEOFFSET())) AS source (tableName, date, records, insertedTimestamp)
                #     ON target.tableName = source.tableName AND target.date = source.date
                #     WHEN MATCHED THEN 
                #         UPDATE SET records = source.records, insertedTimestamp = source.insertedTimestamp
                #     WHEN NOT MATCHED THEN
                #         INSERT (tableName, date, records)
                #         VALUES (source.tableName, source.date, source.records);
                # """

                )

        self.connection.commit()
        return

    def writeManyEntity(self, df: pd.DataFrame, tableName, schemaName, entity, merge=True, mergeKeys: list = None, syncTableName='syncLog', log=True, entityColName='entity', updateInsertedTs=False):
        self._ensure_connection()
        if not df.empty:

            print('Found data')
            cols = df.columns.to_list()
            df[df.select_dtypes(include="bool").columns] = df.select_dtypes(include="bool").astype(int)
            df = df.where(pd.notna(df), None)

            schema = self._quoteIdent(schemaName)
            table = self._quoteIdent(tableName)
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
                    USING (SELECT ? AS {', ? AS '.join(f"? AS {qCols[c]}" for c in cols)}) AS S
                    ON {" AND ".join(f"T.{qCols[c]} = S.{qCols[c]}" for c in mergeKeys)}
                    WHEN MATCHED THEN
                        UPDATE SET
                        {', '.join(f"T.{qCols[c]} = S.{qCols[c]}" for c in [c for c in cols if c not in mergeKeys])}
                        {f", T.insertedTimestamp = SYSDATETIMEOFFSET()" if updateInsertedTs else ""}
                    WHEN NOT MATCHED THEN
                        INSERT ({', '.join(qCols[c] for c in cols)})
                        VALUES ({', '.join(f"S.{qCols[c]}" for c in cols)});
                """


                self.cursorMany.executemany(query, self.parseValues(df))
        print("Committing...")

        if log:
            self.execSingle(
                f"""
                    INSERT INTO {schema}.{self._quoteIdent(syncTableName)}
                        (tableName, {self._quoteIdent(entityColName)}, date, records, insertedTimestamp)
                    VALUES (?, ?, ?, ?, SYSDATETIMEOFFSET());
                """,
                params=[tableName, entity, datetime.now().date(), len(df)]
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
        return self.execSingle(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{schemaName}' AND TABLE_TYPE = 'BASE TABLE'") if True else None


    def constructDatabase(self, configDatabase, schemaName='dbo'):
        existingTables = self.checkExistingDatabase(schemaName, configDatabase['tables']['monitor'][0]['name'])
        if existingTables:
            print("Database bestaat al")
            existingTables = [r[0] for r in existingTables]
        else:
            print("Database bestaat nog niet. Maak aan...")
            querySchema = \
                f"""IF SCHEMA_ID('{schemaName}') IS NULL BEGIN EXEC('CREATE SCHEMA {schemaName}') END"""
            self.execSingle(querySchema)
            existingTables = []

        monitor = configDatabase['tables']['monitor']
        fact = configDatabase['tables']['fact']
        meta = configDatabase['tables']['meta']

        tables = (monitor or []) + (meta or []) + (fact or [])
        tables = [t for t in tables if t['name'] not in existingTables]

        for table in tables:
            constraints = []
            if table.get('constraints'):
                for constraint in table['constraints']:
                    constraints.append(f"CONSTRAINT {table['name']}_{constraint['name']} {constraint['type']} {'CLUSTERED' if constraint['clustered'] else ''} ({', '.join(constraint['columns'])})")


            self.execSingle(
                f"""CREATE TABLE {schemaName}.{table['name']}"""
                f"""({table['columns'].replace('$NAME', table['name'])}{(','+','.join(constraints)) if len(constraints) > 0 else ''})"""
                )
            
            if table.get('indexes'):
                for index in table['indexes']:
                    self.execSingle(f"CREATE INDEX {table['name']}_{index['name']} ON {schemaName}.{table['name']} ({', '.join(index['columns'])})")


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
