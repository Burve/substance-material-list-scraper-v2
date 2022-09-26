"""Access to SQLite database class"""
import sqlite3

from os import path
from sqlite3 import Error
from rich.pretty import pprint


class DatabaseFileDoesNotExist(Exception):
    """Raised when the input value is too small

    Attributes:
        message -- explanation of the error"""

    def __init__(self, db_path, message="Database file '{}' does not exist !!!"):
        self.path = db_path
        self.message = message
        super().__init__(message.format(db_path))


class CommonDatabaseAccess:
    """Class to access SQLite database"""

    def __init__(self, db_path, force):
        """
        Checking if we have our db file
        :param str db_path: path to the database file
        :param bool force: if database file do not exist and force is True, it will be created
        """

        self.conn = None
        self.backup = None
        if not path.exists(db_path):
            if force:
                self.connect_to_database(db_path)
                self.create_database()
            else:
                raise DatabaseFileDoesNotExist(db_path)
        else:
            self.connect_to_database(db_path)

    def __del__(self) -> None:
        """ "Need to close database connection when we are fully done"""
        self.conn.backup(self.backup)
        if self.conn:
            self.conn.close()
        if self.backup:
            self.backup.close()

    def connect_to_database(self, db_path) -> None:
        """Creates connection to the database"""
        try:
            self.backup = sqlite3.connect(
                db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            )
            self.conn = sqlite3.connect(
                ":memory:",
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            )
            self.backup.backup(self.conn)
            self.conn.row_factory = sqlite3.Row
        except Error as _e:
            pprint(_e)
        # finally:
        #     if self.conn:
        #         self.conn.close()

    def create_table(self, create_table_sql) -> None:
        """create a table from the create_table_sql statement
        Attributes:
          create_table_sql: a CREATE TABLE statement

        """
        try:
            _c = self.conn.cursor()
            _c.execute(create_table_sql)
        except Error as _e:
            print(_e)

    def create_database(self) -> None:
        """Creates database structure"""

        sql_create_tag_table = """ CREATE TABLE IF NOT EXISTS tag (
                                    tag_id integer PRIMARY KEY AUTOINCREMENT,
                                    name text NOT NULL
                                    );"""

        sql_create_category_table = """ CREATE TABLE IF NOT EXISTS category (
                                         category_id integer PRIMARY KEY AUTOINCREMENT,
                                         name text NOT NULL
                                         ); """

        sql_create_preview_tag_table = """ CREATE TABLE IF NOT EXISTS preview_tag (
                                           preview_tag_id integer PRIMARY KEY AUTOINCREMENT,
                                           name text NOT NULL
                                           );"""

        sql_create_type_table = """ CREATE TABLE IF NOT EXISTS type (
                                           type_id integer PRIMARY KEY AUTOINCREMENT,
                                           name text NOT NULL
                                           );"""

        sql_create_preview_kind_table = """ CREATE TABLE IF NOT EXISTS preview_kind (
                                            preview_kind_id integer PRIMARY KEY AUTOINCREMENT,
                                            name text NOT NULL
                                            );"""

        sql_create_preview_table = """ CREATE TABLE IF NOT EXISTS preview (
                                      preview_id integer PRIMARY KEY AUTOINCREMENT,
                                      original_id text NOT NULL,
                                      url text NOT NULL,
                                      label text,
                                      preview_kind_id integer NOT NULL,
                                      FOREIGN KEY (preview_kind_id) REFERENCES preview_kind (preview_kind_id)
                                      );"""

        sql_create_preview_preview_tag_table = """ CREATE TABLE IF NOT EXISTS preview_preview_tag (
                                                preview_preview_tag_id integer PRIMARY KEY AUTOINCREMENT,
                                                preview_id integer NOT NULL,
                                                preview_tag_id integer NOT NULL,
                                                FOREIGN KEY (preview_id) REFERENCES preview (preview_id),
                                                FOREIGN KEY (preview_tag_id) REFERENCES preview_tag (preview_tag_id)
                                                );"""

        sql_create_asset_table = """ CREATE TABLE IF NOT EXISTS asset (
                                      asset_id integer PRIMARY KEY AUTOINCREMENT,
                                      original_id text NOT NULL);"""

        sql_create_asset_revision_table = """ CREATE TABLE IF NOT EXISTS asset_revision (
                                      asset_revision_id integer PRIMARY KEY AUTOINCREMENT,
                                      asset_id integer NOT NULL,
                                      name text NOT NULL,
                                      type_id integer NOT NULL,
                                      is_new bool,
                                      is_update bool,
                                      created_at text,
                                      thumbnail_id integer NOT NULL,
                                      extra_data_author text,
                                      extra_data_physical_size text,
                                      extra_data_ref text,
                                      extra_data_type text,
                                      extra_data_style text,
                                      extra_data_quality text,
                                      extra_data_meshes text,
                                      extra_data_counters_quads text,
                                      extra_data_substance_resolution text,
                                      extra_data_preview_disp text,
                                      asset_revision integer NOT NULL,
                                      FOREIGN KEY (asset_id) REFERENCES asset (asset_id),
                                      FOREIGN KEY (type_id) REFERENCES type (type_id),
                                      FOREIGN KEY (thumbnail_id) REFERENCES preview (preview_id)
                                      );"""

        sql_create_asset_tag_table = """ CREATE TABLE IF NOT EXISTS asset_tag (
                                          asset_tag_id integer PRIMARY KEY AUTOINCREMENT,
                                          asset_id integer NOT NULL,
                                          tag_id integer NOT NULL,
                                          FOREIGN KEY (asset_id) REFERENCES asset (asset_id),
                                          FOREIGN KEY (tag_id) REFERENCES tag (tag_id)
                                          );"""

        sql_create_asset_category_table = """ CREATE TABLE IF NOT EXISTS asset_category (
                                          asset_category_id integer PRIMARY KEY AUTOINCREMENT,
                                          asset_id integer NOT NULL,
                                          category_id integer NOT NULL,
                                          is_active bool NOT NULL,
                                          FOREIGN KEY (asset_id) REFERENCES asset (asset_id),
                                          FOREIGN KEY (category_id) REFERENCES category (category_id)
                                          );"""

        sql_create_asset_preview_table = """ CREATE TABLE IF NOT EXISTS asset_preview (
                                          asset_preview_id integer PRIMARY KEY AUTOINCREMENT,
                                          asset_id integer NOT NULL,
                                          preview_id integer NOT NULL,
                                          FOREIGN KEY (asset_id) REFERENCES asset (asset_id),
                                          FOREIGN KEY (preview_id) REFERENCES preview (preview_id)
                                          );"""

        sql_create_download_tag_table = """ CREATE TABLE IF NOT EXISTS download_tag (
                                              download_tag_id integer PRIMARY KEY AUTOINCREMENT,
                                              name text NOT NULL
                                              );"""

        sql_create_download_table = """ CREATE TABLE IF NOT EXISTS download (
                                      download_id integer PRIMARY KEY AUTOINCREMENT,
                                      original_id text NOT NULL,
                                      url text NOT NULL,
                                      label text
                                      );"""

        sql_create_download_download_tag_table = """ CREATE TABLE IF NOT EXISTS download_download_tag (
                                                download_download_tag_id integer PRIMARY KEY AUTOINCREMENT,
                                                download_id integer NOT NULL,
                                                download_tag_id integer NOT NULL,
                                                FOREIGN KEY (download_id) REFERENCES download (download_id),
                                                FOREIGN KEY (download_tag_id) REFERENCES download_tag (download_tag_id)
                                                );"""

        sql_create_revision_table = """ CREATE TABLE IF NOT EXISTS revision (
                                         revision_id integer PRIMARY KEY AUTOINCREMENT,
                                         download_id integer NOT NULL,
                                         filename text NOT NULL,
                                         size integer NUT NULL,
                                         revision integer NOT NULL,
                                         created_at text,
                                         have_file bool,
                                         FOREIGN KEY (download_id) REFERENCES download (download_id)
                                         );"""

        sql_create_asset_download_table = """ CREATE TABLE IF NOT EXISTS asset_download (
                                              asset_download_id integer PRIMARY KEY AUTOINCREMENT,
                                              asset_id integer NOT NULL,
                                              download_id integer NOT NULL,
                                              FOREIGN KEY (asset_id) REFERENCES asset (asset_id),
                                              FOREIGN KEY (download_id) REFERENCES download (download_id)
                                              );"""

        self.create_table(sql_create_tag_table)
        self.create_table(sql_create_category_table)
        self.create_table(sql_create_type_table)
        self.create_table(sql_create_preview_kind_table)
        self.create_table(sql_create_preview_tag_table)
        self.create_table(sql_create_preview_table)
        self.create_table(sql_create_preview_preview_tag_table)
        self.create_table(sql_create_asset_table)
        self.create_table(sql_create_asset_revision_table)
        self.create_table(sql_create_asset_tag_table)
        self.create_table(sql_create_asset_category_table)
        self.create_table(sql_create_asset_preview_table)
        self.create_table(sql_create_download_tag_table)
        self.create_table(sql_create_download_table)
        self.create_table(sql_create_download_download_tag_table)
        self.create_table(sql_create_revision_table)
        self.create_table(sql_create_asset_download_table)

    def get_all_tags(self) -> []:
        """
        Database query for the all saved tags
        :return:
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM tag")

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_new_tag(self, name) -> int:
        """
        Create new tag entry in the database.
        :param str name: name of the tag
        :return: id of the new tag entry
        """
        sql = """INSERT INTO tag (name) VALUES(?)"""
        _c = self.conn.cursor()
        _c.execute(sql, (name,))
        self.conn.commit()
        return _c.lastrowid

    def get_all_preview_kinds(self) -> []:
        """
        Database query for the all saved preview kinds
        :return:
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM preview_kind")

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_new_preview_kind(self, name) -> int:
        """
        Create new preview kind entry in the database.
        :param str name: name of the preview kind
        :return: id of the new tag entry
        """
        sql = """INSERT INTO preview_kind (name) VALUES(?)"""
        _c = self.conn.cursor()
        _c.execute(sql, (name,))
        self.conn.commit()
        return _c.lastrowid

    def get_all_categories(self) -> []:
        """
        Database query for the all saved categories
        :return:
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM category")

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_category_by_id(self, category_id) -> []:
        """
        Database query for the category by id
        :return:
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM category WHERE category_id=?", (category_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_asset_category_by_asset_id_and_category_id(
        self, asset_id, category_id
    ) -> []:
        """
        Database query for the asset_category by asset_id and category_id
        :param int asset_id: ID of the asset
        :param int category_id: ID of the category
        :return: asset category
        """
        _c = self.conn.cursor()
        _c.execute(
            "SELECT * FROM asset_category WHERE asset_id=? AND category_id=?",
            (asset_id, category_id),
        )

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_new_category(self, name) -> int:
        """
        Create new category entry in the database.
        :param str name: name of the category
        :return: id of the new category entry
        """
        sql = """INSERT INTO category (name) VALUES(?)"""
        _c = self.conn.cursor()
        _c.execute(sql, (name,))
        self.conn.commit()
        return _c.lastrowid

    def get_all_preview_tags(self) -> []:
        """
        Database query for the all saved preview tags
        :return:
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM preview_tag")

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_all_preview_tag_by_name(self, name) -> []:
        """
        Database query for the all saved preview tags by tag name
        :return:
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM preview_tag WHERE name=?", (name,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_new_preview_tag(self, name) -> int:
        """
        Create new preview tag entry in the database.
        :param str name: name of the tag
        :return: id of the new tag entry
        """
        sql = """INSERT INTO preview_tag (name) VALUES(?)"""
        _c = self.conn.cursor()
        _c.execute(sql, (name,))
        self.conn.commit()
        return _c.lastrowid

    def get_all_download_tags(self) -> []:
        """
        Database query for the all saved download tags
        :return:
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM download_tag")

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_download_tag_by_download_tag_id(self, download_tag_id) -> []:
        """
        Database query for specific download tag by download tag id
        :return:
        """
        _c = self.conn.cursor()
        _c.execute(
            "SELECT * FROM download_tag WHERE download_tag_id=?", (download_tag_id,)
        )

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_new_download_tag(self, name) -> int:
        """
        Create new download tag entry in the database.
        :param str name: name of the download tag
        :return: id of the new download tag entry
        """
        sql = """INSERT INTO download_tag (name) VALUES(?)"""
        _c = self.conn.cursor()
        _c.execute(sql, (name,))
        self.conn.commit()
        return _c.lastrowid

    def get_all_types(self) -> []:
        """
        Database query for the all saved types
        :return:
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM type")

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_types_by_type_id(self, type_id) -> []:
        """
        Database query for the all saved types by type id
        :return:
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM type WHERE type_id=?", (type_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_new_type(self, name) -> int:
        """
        Create new type entry in the database.
        :param str name: name of the type
        :return: id of the new type entry
        """
        sql = """INSERT INTO type (name) VALUES(?)"""
        _c = self.conn.cursor()
        _c.execute(sql, (name,))
        self.conn.commit()
        return _c.lastrowid

    def get_asset_by_original_id(self, original_id) -> []:
        """
        Database query for the asset by its original ID
        :param string original_id: original ID of the asset
        :return: asset data
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM asset WHERE original_id=?", (original_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_asset_by_asset_id(self, asset_id) -> []:
        """
        Database query for the asset by its asset ID
        :param string asset_id: asset ID of the asset
        :return: asset data
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM asset WHERE asset_id=?", (asset_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_latest_asset_revision_by_original_id(self, original_id) -> []:
        """
        Database query for the latest asset revision by its original ID
        :param string original_id: original ID of the asset
        :return: asset data
        """
        _c = self.conn.cursor()

        sql = """SELECT * FROM asset WHERE original_id=?"""
        _c = self.conn.cursor()
        _c.execute(sql, (original_id,))
        rows = _c.fetchall()
        res = [dict(row) for row in rows]

        if len(res) > 0:
            sql = """SELECT * FROM asset_revision WHERE asset_id=?"""
            _c.execute(sql, (res[0]["asset_id"],))

            rows = _c.fetchall()

            res = [dict(row) for row in rows]

        result = []
        for r in res:
            if len(result) == 0:
                result.append(r)
            elif result[0]["asset_revision"] < r["asset_revision"]:
                result[0] = r

        return result

    def get_asset_revision_by_name(self, name) -> []:
        """
        Database query for the latest asset revision by its name
        :param string name: name of the asset
        :return: asset data
        """

        _c = self.conn.cursor()
        sql = """SELECT * FROM asset_revision WHERE name=?"""
        _c.execute(sql, (name,))

        rows = _c.fetchall()

        res = [dict(row) for row in rows]

        result = []
        for r in res:
            if len(result) == 0:
                result.append(r)
            elif result[0]["asset_revision"] < r["asset_revision"]:
                result[0] = r

        return result

    def get_all_assets_revisions_by_type_id(self, type_id) -> []:
        """
        Database query for all assets by type iD. It is possible to get doubles because of revision!
        :param string type_id: type ID of the asset
        :return: asset data
        """
        _c = self.conn.cursor()
        sql = """SELECT * FROM asset_revision WHERE type_id=?"""
        _c.execute(sql, (type_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_new_asset(self, original_id) -> int:
        """
            Create new asset by Original id
        :param original_id: Original ID of the asset
        :return: id of teh new asset
        """
        sql = """INSERT INTO asset (original_id) VALUES (?)"""
        _c = self.conn.cursor()
        _c.execute(sql, (original_id,))
        self.conn.commit()
        return _c.lastrowid

    def set_new_asset_revision(self, asset_data) -> int:
        """
            Create new asset revision. New asset revision always have Revision as 0
        :param asset_data: asset revision data
        :return: id of the new asset revision
        """
        sql = """INSERT INTO asset_revision (asset_id, name,type_id, is_new, is_update, created_at, thumbnail_id, 
                 extra_data_author, extra_data_physical_size, extra_data_ref, extra_data_type, extra_data_style, 
                 extra_data_quality, extra_data_meshes, extra_data_counters_quads, extra_data_substance_resolution, 
                 extra_data_preview_disp, asset_revision) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)"""
        _c = self.conn.cursor()
        _c.execute(
            sql,
            (
                asset_data["asset_id"],
                asset_data["name"],
                asset_data["type_id"],
                asset_data["is_new"],
                asset_data["is_update"],
                asset_data["created_at"],
                asset_data["thumbnail_id"],
                asset_data["extra_data_author"],
                asset_data["extra_data_physical_size"],
                asset_data["extra_data_ref"],
                asset_data["extra_data_type"],
                asset_data["extra_data_style"],
                asset_data["extra_data_quality"],
                asset_data["extra_data_meshes"],
                asset_data["extra_data_counters_quads"],
                asset_data["extra_data_substance_resolution"],
                asset_data["extra_data_preview_disp"],
            ),
        )
        self.conn.commit()
        return _c.lastrowid

    def update_asset_revision(self, asset_data) -> None:
        """
            Updates asset revision data
        :param asset_data: asset revision data
        """
        sql = """UPDATE asset_revision SET asset_id = ?, name = ?, type_id = ?, is_new = ?, is_update = ?, created_at = ?, 
                 thumbnail_id = ?, extra_data_author = ?, extra_data_physical_size = ?, extra_data_ref = ?, 
                 extra_data_type = ?, extra_data_style = ?, extra_data_quality = ?, extra_data_meshes = ?, 
                 extra_data_counters_quads = ?, extra_data_substance_resolution = ?, extra_data_preview_disp = ?,
                 asset_revision = ? WHERE asset_revision_id = ?"""
        _c = self.conn.cursor()
        _c.execute(
            sql,
            (
                asset_data["asset_id"],
                asset_data["name"],
                asset_data["type_id"],
                asset_data["is_new"],
                asset_data["is_update"],
                asset_data["created_at"],
                asset_data["thumbnail_id"],
                asset_data["extra_data_author"],
                asset_data["extra_data_physical_size"],
                asset_data["extra_data_ref"],
                asset_data["extra_data_type"],
                asset_data["extra_data_style"],
                asset_data["extra_data_quality"],
                asset_data["extra_data_meshes"],
                asset_data["extra_data_counters_quads"],
                asset_data["extra_data_substance_resolution"],
                asset_data["extra_data_preview_disp"],
                asset_data["asset_revision"],
                asset_data["asset_revision_id"],
            ),
        )
        self.conn.commit()

    def update_asset_revision_revision(self, asset_data) -> None:
        """
            Creates new revision of the asset. New revision number is automatically generated.
        :param asset_data: asset revision data
        """
        # First find last revision
        sql = """SELECT * FROM asset_revision WHERE asset_id=?"""
        _c = self.conn.cursor()
        _c.execute(sql, (asset_data["asset_id"],))
        rows = _c.fetchall()

        res = [dict(row) for row in rows]
        last_data = []
        for r in res:
            if len(last_data) == 0:
                last_data.append(r)
            elif last_data[0]["revision"] < r["revision"]:
                last_data[0] = r

        new_revision = 0
        if len(last_data) > 0 and "asset_revision" in last_data[0]:
            new_revision = last_data[0]["asset_revision"] + 1
        # then create new entry
        sql = """INSERT INTO asset_revision (asset_id, name,type_id, is_new, is_update, created_at, thumbnail_id, 
                 extra_data_author, extra_data_physical_size, extra_data_ref, extra_data_type, extra_data_style, 
                 extra_data_quality, extra_data_meshes, extra_data_counters_quads, extra_data_substance_resolution, 
                 extra_data_preview_disp, asset_revision) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        _c = self.conn.cursor()
        _c.execute(
            sql,
            (
                asset_data["asset_id"],
                asset_data["name"],
                asset_data["type_id"],
                asset_data["is_new"],
                asset_data["is_update"],
                asset_data["created_at"],
                asset_data["thumbnail_id"],
                asset_data["extra_data_author"],
                asset_data["extra_data_physical_size"],
                asset_data["extra_data_ref"],
                asset_data["extra_data_type"],
                asset_data["extra_data_style"],
                asset_data["extra_data_quality"],
                asset_data["extra_data_meshes"],
                asset_data["extra_data_counters_quads"],
                asset_data["extra_data_substance_resolution"],
                asset_data["extra_data_preview_disp"],
                new_revision,
            ),
        )
        self.conn.commit()

    def get_all_previews(self) -> []:
        """
        Database query for the preview by its original ID
        :return: preview data
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM preview")

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_preview_by_original_id(self, original_id) -> []:
        """
        Database query for the preview by its original ID
        :param string original_id: original ID of the preview
        :return: preview data
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM preview WHERE original_id=?", (original_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_preview_by_preview_id(self, preview_id) -> []:
        """
        Database query for the preview by its original ID
        :param string preview_id: preview ID of the preview
        :return: preview data
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM preview WHERE preview_id=?", (preview_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_new_preview(self, preview_data) -> int:
        """
            Create new preview.
        :param preview_data: preview data
        :return:
        """
        sql = """INSERT INTO preview (original_id, url, label, preview_kind_id) VALUES (?, ?, ?, ?)"""
        _c = self.conn.cursor()
        _c.execute(
            sql,
            (
                preview_data["original_id"],
                preview_data["url"],
                preview_data["label"],
                preview_data["preview_kind_id"],
            ),
        )
        self.conn.commit()
        return _c.lastrowid

    def get_preview_preview_tag_by_preview_id(self, preview_id) -> []:
        """
        Database query for the preview_tag by preview_id
        :param int preview_id: ID of the preview
        :return: preview tags
        """
        _c = self.conn.cursor()
        _c.execute(
            "SELECT * FROM preview_preview_tag WHERE preview_id=?", (preview_id,)
        )

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_preview_preview_tag_by_preview_id_and_preview_tag_id(
        self, preview_id, preview_tag_id
    ) -> []:
        """
        Database query for the preview_tag by preview_id and preview_tag_id
        :param int preview_id: ID of the preview
        :param int preview_tag_id: ID of the preview tag
        :return: preview tags
        """
        _c = self.conn.cursor()
        _c.execute(
            "SELECT * FROM preview_preview_tag WHERE preview_id=? AND preview_tag_id=?",
            (preview_id, preview_tag_id),
        )

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_all_preview_preview_tags(self) -> []:
        """
        Database query for all preview tags
        :return: preview tags
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM preview_preview_tag")

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_preview_preview_tag(self, preview_id, tag_id) -> int:
        """
            Create new preview preview tag.
        :param preview_id: id of the preview
        :param tag_id: id of the tag
        :return: id of the new preview preview tag entry
        """
        sql = """INSERT INTO preview_preview_tag (preview_id, preview_tag_id) VALUES (?, ?)"""
        _c = self.conn.cursor()
        _c.execute(
            sql,
            (preview_id, tag_id),
        )
        self.conn.commit()
        return _c.lastrowid

    def get_asset_preview_by_asset_id(self, asset_id) -> []:
        """
            Database query for asset preview gy asset id
        :param asset_id: asset id
        :return: asset preview
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM asset_preview WHERE asset_id=?", (asset_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_asset_preview_by_asset_id_and_preview_id(self, asset_id, preview_id) -> []:
        """
        Database query for the asset_preview by asset_id and preview_id
        :param int asset_id: ID of the asset
        :param int preview_id: ID of the preview
        :return: asset preview
        """
        _c = self.conn.cursor()
        _c.execute(
            "SELECT * FROM asset_preview WHERE asset_id=? AND preview_id=?",
            (asset_id, preview_id),
        )

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_all_asset_previews(self) -> []:
        """
        Database query for all asset_preview
        :return: asset preview
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM asset_preview")

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_asset_preview(self, asset_id, preview_id) -> int:
        """
            Creates new asset preview.
        :param asset_id: asset id
        :param preview_id: preview id
        :return: id of the new asset preview
        """
        sql = """INSERT INTO asset_preview (asset_id, preview_id) VALUES (?, ?)"""
        _c = self.conn.cursor()
        _c.execute(
            sql,
            (asset_id, preview_id),
        )
        self.conn.commit()
        return _c.lastrowid

    def get_asset_category_by_asset_id(self, asset_id) -> []:
        """
            Database query for all asset categories by asset id
        :param asset_id: asset id
        :return: asset category
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM asset_category WHERE asset_id=?", (asset_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_active_asset_category_by_asset_id(self, asset_id) -> []:
        """
            Database query for active asset category by asset id
        :param asset_id: asset id
        :return: asset category
        """
        _c = self.conn.cursor()
        _c.execute(
            "SELECT * FROM asset_category WHERE asset_id=? AND is_active", (asset_id,)
        )

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_asset_category(self, asset_id, category_id, is_active) -> int:
        """
            Create asset category.
        :param asset_id: asset id
        :param category_id: category id
        :param is_active: is category active for this asset
        :return: id of teh new asset category
        """
        sql = """INSERT INTO asset_category (asset_id, category_id, is_active) VALUES (?, ?, ?)"""
        _c = self.conn.cursor()
        _c.execute(
            sql,
            (asset_id, category_id, is_active),
        )
        self.conn.commit()
        return _c.lastrowid

    def update_asset_category(self, asset_category_data) -> None:
        """
            Update asset category. Used to set it's is_active status.
        :param asset_category_data: asset category data.
        """
        sql = """UPDATE asset_category set asset_id = ?, category_id = ?, is_active = ?
                 WHERE asset_category_id = ?"""
        _c = self.conn.cursor()
        _c.execute(
            sql,
            (
                asset_category_data["asset_id"],
                asset_category_data["category_id"],
                asset_category_data["is_active"],
                asset_category_data["asset_category_id"],
            ),
        )
        self.conn.commit()

    def get_asset_tag_by_asset_id(self, asset_id) -> []:
        """
            Database query for asset tag by asset id.
        :param asset_id: asset id
        :return: asset tag
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM asset_tag WHERE asset_id=?", (asset_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_asset_tag_by_asset_id_and_tag_id(self, asset_id, tag_id) -> []:
        """
        Database query for the asset_tag by asset_id and tag_id
        :param int asset_id: ID of the asset
        :param int tag_id: ID of the tag
        :return: asset tags
        """
        _c = self.conn.cursor()
        _c.execute(
            "SELECT * FROM asset_tag WHERE asset_id=? AND tag_id=?", (asset_id, tag_id)
        )

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_all_asset_tags(self) -> []:
        """
        Database query for all asset_tag
        :return: asset tags
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM asset_tag")

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_asset_tag(self, asset_id, tag_id) -> int:
        """
            Create asset tag
        :param asset_id: asset id
        :param tag_id: tag id
        :return: id of teh created asset tag
        """
        sql = """INSERT INTO asset_tag (asset_id, tag_id) VALUES (?, ?)"""
        _c = self.conn.cursor()
        _c.execute(
            sql,
            (asset_id, tag_id),
        )
        self.conn.commit()
        return _c.lastrowid

    def get_asset_download_by_asset_id(self, asset_id) -> []:
        """
            Database query for asset download by asset id
        :param asset_id: asset id
        :return: asset download
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM asset_download WHERE asset_id=?", (asset_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_asset_download_by_download_id(self, download_id) -> []:
        """
            Database query for asset download by download id
        :param download_id: download id
        :return: asset download
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM asset_download WHERE download_id=?", (download_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_asset_download_by_asset_id_and_download_id(
        self, asset_id, download_id
    ) -> []:
        """
        Database query for the asset_download by asset_id and download_id
        :param int asset_id: ID of the asset
        :param int download_id: ID of the download
        :return: asset download
        """
        _c = self.conn.cursor()
        _c.execute(
            "SELECT * FROM asset_download WHERE asset_id=? AND download_id=?",
            (asset_id, download_id),
        )

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_asset_download(self, asset_id, download_id) -> int:
        """
            Create asset download.
        :param asset_id: asset id
        :param download_id: download id
        :return: id of the new asset download
        """
        sql = """INSERT INTO asset_download (asset_id, download_id) VALUES (?, ?)"""
        _c = self.conn.cursor()
        _c.execute(
            sql,
            (asset_id, download_id),
        )
        self.conn.commit()
        return _c.lastrowid

    def get_download_download_tag_by_download_id(self, download_id) -> []:
        """
            Database query for download download tag by download id
        :param download_id: download id
        :return: download download tag
        """
        _c = self.conn.cursor()
        _c.execute(
            "SELECT * FROM download_download_tag WHERE download_id=?", (download_id,)
        )

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_download_download_tag_by_download_id_and_download_tag_id(
        self, download_id, download_tag_id
    ) -> []:
        """
        Database query for the download_download_tag by download_id and download_tag_id
        :param int download_id: ID of the download
        :param int download_tag_id: ID of the download preview tag
        :return: download tags
        """
        _c = self.conn.cursor()
        _c.execute(
            "SELECT * FROM download_download_tag WHERE download_id=? AND download_tag_id=?",
            (download_id, download_tag_id),
        )

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_download_download_tag(self, download_id, download_tag_id) -> int:
        """
            Create download download tag
        :param download_id: download id
        :param download_tag_id: download tag id
        :return:
        """
        sql = """INSERT INTO download_download_tag (download_id, download_tag_id) VALUES (?, ?)"""
        _c = self.conn.cursor()
        _c.execute(
            sql,
            (download_id, download_tag_id),
        )
        self.conn.commit()
        return _c.lastrowid

    def get_download_by_original_id(self, original_id) -> []:
        """
        Database query for the download by its original ID
        :param string original_id: original ID of the download
        :return: download data
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM download WHERE original_id=?", (original_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_download_by_download_id(self, download_id) -> []:
        """
        Database query for the download by its original ID
        :param string download_id: download ID
        :return: download data
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM download WHERE download_id=?", (download_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_new_download(self, download_data) -> int:
        """
            Create new download
        :param download_data: download data
        :return: id of the new download
        """
        sql = """INSERT INTO download (original_id, url, label) VALUES (?, ?, ?)"""
        _c = self.conn.cursor()
        _c.execute(
            sql,
            (
                download_data["original_id"],
                download_data["url"],
                download_data["label"],
            ),
        )
        self.conn.commit()
        return _c.lastrowid

    def get_revision_by_download_id(self, download_id) -> []:
        """
        Database query for the revision by download ID
        :param string download_id: download ID of the revision
        :return: revision data
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM revision WHERE download_id=?", (download_id,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_revision_by_filename(self, filename) -> []:
        """
        Database query for the revision by its file name
        :param string filename: file name associated with this revision
        :return: revision data
        """
        _c = self.conn.cursor()
        _c.execute("SELECT * FROM revision WHERE filename=?", (filename,))

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def get_revisions_by_download_id_and_revision(self, download_id, revision) -> []:
        """
        Database query for the revisions by download_id and revision
        :param int download_id: ID of the download
        :param int revision: revision
        :return: revision data
        """
        _c = self.conn.cursor()
        _c.execute(
            "SELECT * FROM revision WHERE download_id=? AND revision=?",
            (download_id, revision),
        )

        rows = _c.fetchall()

        return [dict(row) for row in rows]

    def set_new_revision(self, revision_data) -> int:
        """
            Create new revision
        :param revision_data: revision data
        :return: id of the new revision
        """
        sql = """INSERT INTO revision (download_id, filename, size, revision, created_at, have_file) 
                 VALUES (?, ?, ?, ?, ?, ?)"""
        _c = self.conn.cursor()
        _c.execute(
            sql,
            (
                revision_data["download_id"],
                revision_data["filename"],
                revision_data["size"],
                revision_data["revision"],
                revision_data["created_at"],
                revision_data["have_file"],
            ),
        )
        self.conn.commit()
        return _c.lastrowid

    def update_revision(self, revision_data) -> None:
        """
            Update revision.
        :param revision_data: revision data
        """
        sql = """UPDATE revision SET download_id = ?, filename = ?, size = ?, revision = ?, created_at = ?, have_file = ?
                 WHERE revision_id = ?"""
        _c = self.conn.cursor()
        _c.execute(
            sql,
            (
                revision_data["download_id"],
                revision_data["filename"],
                revision_data["size"],
                revision_data["revision"],
                revision_data["created_at"],
                revision_data["have_file"],
                revision_data["revision_id"],
            ),
        )
        self.conn.commit()

    def get_latest_revision_by_download_id(self, download_id) -> []:
        """
        Database query for the latest revision of the download id. Can have doubles
        :param integer download_id: download ID of the revision
        :return: revision data
        """
        _c = self.conn.cursor()

        sql = """SELECT * FROM revision WHERE download_id=?"""
        _c.execute(sql, (download_id,))

        rows = _c.fetchall()

        res = [dict(row) for row in rows]
        result = []
        for r in res:
            if len(result) == 0:
                result.append(r)
            elif result[0]["revision"] < r["revision"]:
                result[0] = r

        return result
