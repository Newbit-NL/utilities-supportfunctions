# import requests
import os
import re
from contextlib import contextmanager
from datetime import datetime
from time import monotonic, sleep
from typing import Literal

import pandas as pd
import numpy as np
import yaml


#init_check checked of de check table al bestaat. Zo nee, dan maakt hij alle benodigde tabellen aan, het schema, etc.

# Moet ruim onder de tijd liggen waarna de server of een firewall een stille verbinding verbreekt
_PING_AFTER_SECONDS = 30

def _isNa(value):
    # pd.isna() raises ValueError on array-like values (list/tuple/dict/ndarray) because
    # it returns an element-wise array instead of a single bool; such values are never "missing"
    if isinstance(value, (list, tuple, set, dict, np.ndarray)):
        return False
    return pd.isna(value)


class print2():
    """Styled printer that keeps an in-memory log of everything it prints.

    The style codes let you build up multi-level structured output (headers with
    nested bullets and results) without hand-formatting indentation each time. Example::

        p = print2()
        p.print(1, "Step 1: extract")           # top-level header
        p.print(2, "reading source table")      # level 1 bullet under it
        p.print(3, "42 rows fetched")            # level 1 result under that bullet
        p.print(5, "Step 1a: validation")        # level 2 header (sub-section)
        p.print(7, "checking nulls")             # level 2 bullet under it
        p.print(9, "no nulls found")             # level 2 result under that bullet
        p.print(10)                              # divider

    prints::

        # Step 1: extract
        \t-reading source table
        \t\t-> 42 rows fetched

        ## Step 1a: validation
        \t\t-checking nulls
        \t\t\t-> no nulls found
        ------------------------------------------------------------

    Style reference (style code -> layout, typical use):
        0  - plain text, no prefix                                 freeform output
        1  - level 1 header:   blank line + "# "                   top-level step/section
        2  - level 1 bullet:   "\\t-"                                item under a level 1 header
        3  - level 1 result:   "\\t\\t-> "                            outcome of a level 1 bullet
        4  - inline header:    "\\t# " (no blank line)             indented header right after another line
        5  - level 2 header:   blank line + "## "                  sub-section under a level 1 header
        6  - level 3 header:   blank line + "### "                 sub-section under a level 2 header
        7  - level 2 bullet:   "\\t\\t-"                             item under a level 2 header
        8  - level 3 bullet:   "\\t\\t\\t-"                            item under a level 2 bullet
        9  - level 2 result:   "\\t\\t\\t-> "                          outcome of a level 2/3 bullet
        10 - divider:          "-" * 60 (text is ignored)          visual separator between sections

    Each call to `print()` writes to stdout with the selected style and appends the
    resulting line to an in-memory log, which can later be flushed to a .txt file via
    `saveLog`. `saveLog` clears the log automatically once it has been written, so the
    in-memory log only ever holds lines printed since the last save (or since `clearLog`).
    """

    _STYLES: dict[int, str] = {
        0: '{text}',
        1: '\n# {text}',
        2: '\t-{text}',
        3: '\t\t-> {text}',
        4: '\t# {text}',
        5: '\n## {text}',
        6: '\n### {text}',
        7: '\t\t-{text}',
        8: '\t\t\t-{text}',
        9: '\t\t\t-> {text}',
        10: '-' * 60,
    }

    def __init__(self):
        self._memory: list[str] = []

    def print(self, style: int, text: str = '') -> None:
        """Print text with a predefined visual style and log it in memory.

        See the class docstring for the full style reference table (styles 0-10),
        covering plain text, three levels of headers/bullets/results, and a divider.

        Args:
            style: Selected style code (0-10).
            text: Text to print. Defaults to ''. Ignored for style 10 (divider).

        Raises:
            ValueError: If style is not one of the supported style codes.
        """
        try:
            template = self._STYLES[style]
        except KeyError:
            raise ValueError(
                f"print2.print: invalid style {style!r}, expected one of {sorted(self._STYLES)}"
            ) from None
        line = template.format(text=text)
        print(line)
        self._memory.append(line)

    def clearLog(self) -> None:
        """Discard the in-memory log without saving it."""
        self._memory = []

    def saveLog(self, path: str, filename: str) -> str:
        """Write the in-memory log to a .txt file, then clear the in-memory log.

        Args:
            path: Directory to save the log file in. Created if it doesn't exist.
            filename: File name for the log, with or without a '.txt' extension.

        Returns:
            The full path of the written log file.
        """
        os.makedirs(path, exist_ok=True)
        if not filename.lower().endswith('.txt'):
            filename += '.txt'
        fullPath = os.path.join(path, filename)
        with open(fullPath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(self._memory))
        self.clearLog()
        return fullPath


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
                    if _isNa(row[col]):
                        d.append(None)
                    else:
                        d.append(str(row[col]))
                else:
                    if _isNa(row[col]):
                        d.append(None)
                    elif isinstance(row[col], np.integer):
                        d.append(int(row[col]))
                    elif isinstance(row[col], np.floating):
                        d.append(float(row[col]))
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
                self.execSingle(f"CREATE {'CLUSTERED' if index.get('clustered', False) else 'NONCLUSTERED'} INDEX {qIndexName} ON {qSchema}.{qTable} ({qIndexCols}) {includeClause}")

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
    """Postgres-verbinding (psycopg2) voor ETL-werk.

    Standaard commit elke execSingle/writeMany/writeManyEntity zelf, en herstelt
    _ensure_connection vóór elke aanroep een afgebroken transactie of verbroken verbinding.

    Transacties: meerdere statements atomair uitvoeren met

        with sqlc.transaction():
            sqlc.execSingle("SELECT set_config('app.user', %s, true)", ["piet"])
            sqlc.execSingle("INSERT ...")

    - Binnen het blok committen execSingle, writeMany en writeManyEntity niet zelf; aan het
      eind van het blok volgt één commit. Bij een exception volgt een rollback en wordt de
      exception opnieuw gegooid.
    - Alleen bij het begin van het blok wordt (zo nodig) gepingd of opnieuw verbonden. Binnen
      het blok wordt nooit stilletjes opnieuw verbonden, want dan gaan eerder werk en
      SET LOCAL / set_config(..., true) verloren: is de verbinding weg of de transactie
      afgebroken, dan volgt een psycopg2.OperationalError / InternalError.
    - Geneste transaction() wordt niet ondersteund en geeft een RuntimeError.

    execSingle(..., return_as='rowcount') geeft cursor.rowcount terug (aantal geraakte rijen).
    """

    def __init__(self, configSql, sqlUser=None, sqlPassword=None):
        self.configSql = configSql
        self.connectionParams = self.constructConnParams(configSql, sqlUser, sqlPassword)
        self.open = None
        self._inTransaction = False
        self.connection = self.openConnection()
        self.cursorSingle = self.get_cursor()
        self.cursorMany = self.get_cursor(fast=True)
        self._lastUsed = monotonic()

    def __str__(self):
        return f"postgresConnection(server='{self.configSql['server']}', database='{self.configSql['database']}', open={'True' if self.open else 'False'})"

    def constructConnParams(self, configSql, sqlUser=None, sqlPassword=None):
        c = configSql
        configType = c.get("configType", "local-dev-local")
        # server-depl-server, local-dev-local, local-dev-server

        params = {
            "host": "localhost" if configType in ["server-depl-server", 'local-dev-local'] else c["server"],
            "port": c.get("port", 5432),
            "dbname": c["database"],
            "connect_timeout": 30,
        }


        if configType in ["server-depl-server", 'local-dev-server']:
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

    def _commit(self):
        # binnen transaction() commit alleen het einde van het blok
        if not self._inTransaction:
            self.connection.commit()
        self._lastUsed = monotonic()

    @contextmanager
    def transaction(self):
        """Voer alle statements in het blok uit als één transactie: één commit aan het eind,
        rollback (en exception opnieuw gooien) bij een fout. Niet nestbaar."""
        if self._inTransaction:
            raise RuntimeError("Geneste postgresConnection.transaction() wordt niet ondersteund")

        # enige moment waarop gepingd / opnieuw verbonden mag worden
        self._ensure_connection()
        self._inTransaction = True
        try:
            yield self
        except BaseException:
            try:
                self.connection.rollback()
            except Exception:
                pass  # verbinding kan al weg zijn; de oorspronkelijke fout is belangrijker
            raise
        else:
            self.connection.commit()
            self._lastUsed = monotonic()
        finally:
            self._inTransaction = False

    def _reconnect(self):
        print("Herstellen verbinding met Postgres server")
        try:
            self.connection.close()  # anders houdt de oude verbinding een plek in max_connections bezet
        except Exception:
            pass
        self.connection = self.openConnection()
        self.cursorSingle = self.get_cursor()
        self.cursorMany = self.get_cursor(fast=True)
        self._lastUsed = monotonic()

    def _ensure_connection(self):
        """Herstel een afgebroken transactie of verbroken verbinding, en ping alleen als de
        verbinding langer dan _PING_AFTER_SECONDS stil lag (een ping kost een round-trip per query)."""
        import psycopg2
        from psycopg2 import extensions as pgExt

        if self._inTransaction:
            # nooit stilletjes reconnecten of rollbacken: dan gaat het werk van eerdere
            # statements en elke SET LOCAL / set_config(..., true) in het blok verloren
            if self.connection is None or self.connection.closed:
                raise psycopg2.OperationalError("Verbinding verbroken binnen postgresConnection.transaction()")
            status = self.connection.get_transaction_status()
            if status == pgExt.TRANSACTION_STATUS_UNKNOWN:
                raise psycopg2.OperationalError("Verbinding verbroken binnen postgresConnection.transaction()")
            if status == pgExt.TRANSACTION_STATUS_INERROR:
                raise psycopg2.InternalError(
                    "Transactie is afgebroken door een eerdere fout binnen postgresConnection.transaction()"
                )
            return

        if self.connection is None or self.connection.closed:
            self._reconnect()
            return

        try:
            status = self.connection.get_transaction_status()
            if status == pgExt.TRANSACTION_STATUS_UNKNOWN:
                self._reconnect()
                return
            if status == pgExt.TRANSACTION_STATUS_INERROR:
                self.connection.rollback()
                status = pgExt.TRANSACTION_STATUS_IDLE

            if monotonic() - self._lastUsed < _PING_AFTER_SECONDS:
                return

            self.cursorSingle.execute("SELECT 1")
            self.cursorSingle.fetchone()
            if status == pgExt.TRANSACTION_STATUS_IDLE:
                # alleen de transactie van de ping zelf afsluiten, niet ongecommit werk van de aanroeper
                self.connection.rollback()
            self._lastUsed = monotonic()
        except Exception:
            self._reconnect()

    def execSingle(self, query, params=None, return_as: Literal["dict", "df", "list", "single", "rowcount"] | None = None, returnColumns=False):
            self._ensure_connection()

            self.cursorSingle.execute(query, params or ())
            if return_as == "rowcount":
                rowcount = self.cursorSingle.rowcount
                if self.cursorSingle.description is not None:
                    self.cursorSingle.fetchall()
                self._commit()
                return rowcount

            if self.cursorSingle.description is not None:
                rows = self.cursorSingle.fetchall()
                columns = [desc[0] for desc in self.cursorSingle.description]
                self._commit()

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
                self._commit()
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

            if merge:
                # ON CONFLICT DO UPDATE mag dezelfde doelrij niet twee keer in één statement raken;
                # net als de row-by-row MERGE bij mssql wint de laatste rij per mergeKeys-combinatie
                keyIdx = [cols.index(c) for c in mergeKeys]
                deduped = {}
                for r in rows:
                    deduped[tuple(r[i] for i in keyIdx)] = r
                rows = list(deduped.values())

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
                    setClause += f", {self._quoteIdent('insertedTimestamp')} = now()"
                query = (
                    f"INSERT INTO {schema}.{table} ({', '.join(qCols[c] for c in cols)}) VALUES %s "
                    f"ON CONFLICT ({', '.join(qCols[c] for c in mergeKeys)}) DO UPDATE SET {setClause}"
                )
                execute_values(self.cursorMany, query, rows, page_size=10000)
            self._commit()
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

        self._commit()
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

            if merge:
                # ON CONFLICT DO UPDATE mag dezelfde doelrij niet twee keer in één statement raken;
                # net als de row-by-row MERGE bij mssql wint de laatste rij per mergeKeys-combinatie
                keyIdx = [cols.index(c) for c in mergeKeys]
                deduped = {}
                for r in rows:
                    deduped[tuple(r[i] for i in keyIdx)] = r
                rows = list(deduped.values())

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
                    setClause += f", {self._quoteIdent('insertedTimestamp')} = now()"
                query = (
                    f"INSERT INTO {schema}.{table} ({', '.join(qCols[c] for c in cols)}) VALUES %s "
                    f"ON CONFLICT ({', '.join(qCols[c] for c in mergeKeys)}) DO UPDATE SET {setClause}"
                )
                execute_values(self.cursorMany, query, rows, page_size=10000)
            self._commit()
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
        self._commit()
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
                    d.append(bool(value) if not _isNa(value) else None)
                elif df[col].dtype == 'object':
                    if _isNa(value):
                        d.append(None)
                    else:
                        d.append(str(value))
                else:
                    if _isNa(value):
                        d.append(None)
                    elif isinstance(value, np.integer):
                        # numpy.int64 heeft geen psycopg2-adapter; alleen zichtbaar als de rij puur integer is
                        d.append(int(value))
                    elif isinstance(value, np.floating):
                        d.append(float(value))
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
