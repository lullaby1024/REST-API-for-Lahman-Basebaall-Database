# Qi Feng (qf2140)
from aeneid.dbservices.BaseDataTable import BaseDataTable
from aeneid.dbservices.DerivedDataTable import DerivedDataTable
import pandas as pd

import pymysql
import logging


class RDBDataTable(BaseDataTable):
    """
    RDBDataTable is relation DB implementation of the BaseDataTable.
    """

    # Default connection information in case the code does not pass an object
    # specific connection on object creation.
    #
    # You must replace with your own schema, user id and password.
    #
    _default_connect_info = {
        'host': 'localhost',
        'user': 'root',
        'password': 'mydbuser',
        'db': 'lahman2017',
        'port': 3306
    }

    def __init__(self, table_name, key_columns=None, connect_info=None, debug=True):
        """

        :param table_name: The name of the RDB table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """

        # Initialize and store information in the parent class.
        super().__init__(table_name, connect_info, key_columns, debug)

        # If there is not explicit connect information, use the defaults.
        if connect_info is None:
            self._connect_info = RDBDataTable._default_connect_info

        # Create a connection to use inside this object. In general, this is not the right approach.
        # There would be a connection pool shared across many classes and applications.
        self._cnx = pymysql.connect(
            host=self._connect_info['host'],
            user=self._connect_info['user'],
            password=self._connect_info['password'],
            db=self._connect_info['db'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

        self._key_columns = self._get_primary_key_columns()
        self._related_resources = None

        tn = self._table_name.split('.')
        if len(tn) == 1:
            self._schema = self._connect_info['db']
            self._table = tn[0]
        else:
            self._schema = tn[0]
            self._table = tn[1]

        self._columns = self._get_columns()

    def debug_message(self, *m):
        """
        Prints some debug information if self._debug is True
        :param m: List of things to print.
        :return: None
        """
        if self._debug:
            print(" *** DEBUG:", *m)

    def __str__(self):
        """

        :return: String representation of the data table.
        """
        result = "RDBDataTable: table_name = " + self._table_name
        result += "\nTable type = " + str(type(self))
        result += "\nKey fields: " + str(self._key_columns)

        # Find out how many rows are in the table.
        q1 = "SELECT count(*) as count from " + self._table_name
        r1 = self._run_q(q1, fetch=True, commit=True)
        result += "\nNo. of rows = " + str(r1[0]['count'])

        # Get the first five rows and print to show sample data.
        # Query to get data.
        q = "select * from " + self._table_name + " limit 5"

        # Read into a data frame to make pretty print easier.
        df = pd.read_sql(q, self._cnx)
        result += "\nFirst five rows:\n"
        result += df.to_string()

        return result

    def _run_q(self, q, args=None, fields=None, fetch=True, cnx=None, commit=True):
        """

        :param q: An SQL query string that may have %s slots for argument insertion. The string
            may also have {} after select for columns to choose.
        :param args: A tuple of values to insert in the %s slots.
        :param fetch: If true, return the result.
        :param cnx: A database connection. May be None
        :param commit: Do not worry about this for now. This is more wizard stuff.
        :return: A result set or None.
        """

        # Use the connection in the object if no connection provided.
        if cnx is None:
            cnx = self._cnx

        # Convert the list of columns into the form "col1, col2, ..." for following SELECT.
        if fields:
            q = q.format(",".join(fields))

        cursor = cnx.cursor()  # Just ignore this for now.

        # If debugging is turned on, will print the query sent to the database.
        self.debug_message("Query = ", cursor.mogrify(q, args))

        r = cursor.execute(q, args)  # Execute the query.

        # Technically, INSERT, UPDATE and DELETE do not return results.
        # Sometimes the connector libraries return the number of created/deleted rows.
        if fetch:
            r = cursor.fetchall()  # Return all elements of the result.
        else:
            # r = None
            pass

        if commit:  # Do not worry about this for now.
            cnx.commit()

        return r

    def _run_insert(self, table_name, column_list, values_list, cnx=None, commit=False):
        """

        :param table_name: Name of the table to insert data. Probably should just get from the object data.
        :param column_list: List of columns for insert.
        :param values_list: List of column values.
        :param cnx: Ignore this for now.
        :param commit: Ignore this for now.
        :return:
        """
        try:
            q = "insert into " + table_name + " "

            # If the column list is not None, form the (col1, col2, ...) part of the statement.
            if column_list is not None:
                q += "(" + ",".join(column_list) + ") "

            # We will use query parameters. For a term of the form values(%s, %s, ...) with one slot for
            # each value to insert.
            values = ["%s"] * len(values_list)

            # Form the values(%s, %s, ...) part of the statement.
            values = " ( " + ",".join(values) + ") "
            values = "values" + values

            # Put all together.
            q += values

            self._run_q(q, args=values_list, fields=None, fetch=False, cnx=cnx, commit=True)

        except Exception as e:
            print("Got exception = ", e)
            raise e

    def _get_foreign_key(self, related_resource):
        q = """
            SELECT
              CONSTRAINT_NAME,
              TABLE_NAME,
              COLUMN_NAME,
              REFERENCED_TABLE_NAME,
              REFERENCED_COLUMN_NAME,
              ORDINAL_POSITION,
              POSITION_IN_UNIQUE_CONSTRAINT
                FROM
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE
                    (REFERENCED_TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME = %s
                        AND TABLE_SCHEMA = %s AND TABLE_NAME = %s)
                    OR
                    (REFERENCED_TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME = %s
                        AND TABLE_SCHEMA = %s AND TABLE_NAME = %s)

        """

        args = (self._schema, related_resource._table, self._schema, self._table,
                self._schema, self._table, self._schema, related_resource._table)

        foreign_key = self._run_q(q, args)

        return foreign_key[0]['COLUMN_NAME']

    def _get_primary_key_columns(self):
        """

        :return: The names of the primary key columns in the form ['col1', ..., 'coln']

        The current implementation just returns the list of keys passed in the constructor.
        An extended implementation would/should query database data/metadata to get the information.
        """
        return self._key_columns

    def _get_columns(self):
        q = """
        select *
        from INFORMATION_SCHEMA.COLUMNS
        where
        TABLE_NAME = %s
        """
        result = self._run_q(q, self._table)
        col = []

        for r in result:
            col.append(r['COLUMN_NAME'])

        return col

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the request fields for the record identified
            by the key.
        """

        # Get the key_columns specified on table create.
        key_columns = self._get_primary_key_columns()

        # Zipping together key_columns and passed fields produces a valid template
        tmp = dict(zip(key_columns, key_fields))

        # Call find_by_template. This returns a DerivedDataTable.
        result = self.find_by_template(tmp, field_list)

        # For various reasons, we do not return a DerivedDataTable for find_by_primary_key().
        # We return the single row. This follows REST semantics.
        if result is not None:
            result = result.get_rows()

            if result is not None and len(result) > 0:
                result = result[0]
            else:
                result = None

        return result

    def _template_to_where_clause(self, t):
        """
        Convert a query template into a WHERE clause.
        :param t: Query template.
        :return: (WHERE clause, arg values for %s in clause)
        """
        terms = []
        args = []
        w_clause = ""

        # The where clause will be of the for col1=%s, col2=%s, ...
        # Build a list containing the individual col1=%s
        # The args passed to +run_q will be the values in the template in the same order.
        for k, v in t.items():
            temp_s = k + "=%s "
            terms.append(temp_s)
            args.append(v)

        if len(terms) > 0:
            w_clause = "WHERE " + " AND ".join(terms)
        else:
            w_clause = ""
            args = None

        return w_clause, args

    def _get_extras(self, limit=None, offset=None, order_by=None):
        """
        Handle the extra q that is not covered before
        :param limit
        :param offset
        :param order_by
        :return: a string
        """
        result = ' '

        if order_by:
            result += ' order by ' + order_by + ' '
        if limit:
            result += ' limit ' + str(limit)
        if offset:
            result += ' offset ' + str(offset)

        return result

    # def _project(self, rows, field_list):
    #     return rows
    #
    #     """
    #     result = []
    #
    #     if field_list is None:
    #         result = rows
    #     else:
    #         for r in rows:
    #             new_r = {f: r[f] for f in field_list}
    #             result.append(new_r)
    #
    #     return result
    #     """

    # def _get_join_clause(self, parent, children):
    #
    #     result_terms = []
    #
    #     for k,v in self._related_resources.items():
    #         if (v["TABLE_NAME"].lower() == parent and v["REFERENCED_TABLE_NAME"].lower() == children) \
    #             or \
    #                 (v["TABLE_NAME"].lower() == children and v["REFERENCED_TABLE_NAME"].lower() == parent):
    #
    #             for m in v['MAP']:
    #                 result_terms.append(v['TABLE_NAME'] + '.' + m[0] + "=" + \
    #                     v['REFERENCED_TABLE_NAME'] + "." + m[1])
    #             break
    #         if result_terms:
    #             result = " AND ".join(result_terms)
    #         else:
    #             result = None
    #
    #         return result

    def _get_join_clause(self, parent, children):

        foreign_key = self._get_foreign_key(RDBDataTable(children))
        result = parent + '.' + foreign_key + "=" + children + "." + foreign_key

        return result

    def _post_process_join(self, parent, children, result):
        col = self._columns

        final_result = []

        for sub_r in result:
            result_terms = {parent: {}, children: {}}
            for k, v in sub_r.items():
                if k in col:
                    result_terms[parent][k] = v
                else:
                    result_terms[children][k] = v

            final_result.append(result_terms)

        return final_result

    def find_by_template(self, template, field_list=None, limit=None,
                         offset=None, order_by=None, follow_paths=False,
                         commit=True):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """

        result = None

        try:
            # Compute the where clause.
            w_clause = self._template_to_where_clause(template)

            if field_list is None:
                # If there is not field list, we are doing 'select *'
                f_select = ['*']
            else:
                f_select = field_list

            ext = self._get_extras(limit, offset, order_by)

            # Query template.
            # _run_q will use args for %s terms and will format the field selection into {}
            q = "select {} from " + self._table_name + " " + w_clause[0] + " " + ext

            result = self._run_q(q, args=w_clause[1], fields=f_select, fetch=True, commit=commit)

            # result = self._project(result, field_list)

            # SELECT queries always produce tables.
            result = DerivedDataTable("SELECT(" + self._table_name + ")", result)

        except Exception as e:
            logging.error("RDBDataTable.find_by_template_exception", exc_info=True)
            raise e

        return result

    def find_by_path_template_pair(self, parent, children=None, template=None,
                              field_list=None, limit=None, offset=None, order_by=None):
        result = None
        jcs = []

        try:
            q = "select {columns}\n from {tables}\n {on}\n {where}\n {extras}"
            on_c = None

            # if children:
            #     field_list = self._add_aliases(field_list)
            jc = self._get_join_clause(parent, children)
            on_c = jc
            tables = " " + parent + "," + children + " "

            wc = self._template_to_where_clause(template)

            if on_c is not None:
                w_string = wc[0] + " and " + on_c
            else:
                on_c = " "
                w_string = wc[0]

            extras = self._get_extras(limit=limit, offset=offset, order_by=order_by)

            q = q.format(columns=field_list, tables=tables, on=" ", where=w_string, extras=extras)

            result = self._run_q(q, wc[1], fetch=True)

            if children:
                result = self._post_process_join(parent, children, result)

        except Exception as e:
            logging.error("find_by_path_template_pair_exception", exc_info=True)
            raise e

        return result

    def _get_specific_template(self, table_name, template):

        tmp = {}
        for k,v in template.items():
            if table_name.lower() in k:
                string = table_name + "."
                tmp[k.replace(string, "")] = v

        return tmp

    def find_by_path_template(self, children=None, template=None,
                              field_list=None, limit=None, offset=None, order_by=None):
        field_array = None
        field_array = field_list
        parent = self._table

        try:
            if children is None:
                return self.find_by_template(template, field_list, limit, offset, order_by)
            else:
                result = None
                parent_template = self._get_specific_template(parent, template)
                parent_fields = [f for f in field_array if (parent + ".") in f]

                for c in children:
                    child_template = self._get_specific_template(c, template)
                    child_fields = [f for f in field_array if (c + ".") in f]

                    if child_template is not None:
                        all_template = {**parent_template, **child_template}
                    else:
                        all_template = parent_template

                    all_fields = ",".join(parent_fields + child_fields)

                    t = self.find_by_path_template_pair(parent, c, all_template, all_fields,
                                                        limit, offset, order_by)
                    if result is None:
                        result = t
                    else:
                        for sub_r in result:
                            for sub_t in t:
                                rp_s = str(sub_r[parent])
                                tp_s = str(sub_t[parent])

                                if tp_s == rp_s:
                                    sub_r[c] = sub_t[c]
                                    break

        except Exception as e:
            logging.error("RDBDataTable.find_by_template_path_exception", exc_info=True)
            raise e

        return result

    def delete_by_template(self, template):
        """

        Deletes all records that match the template.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        result = None

        try:
            # Compute the where clause.
            w_clause = self._template_to_where_clause(template)

            # Form delete query
            q = "delete from " + self._table_name + " " + w_clause[0]

            # Execute
            result = self._run_q(q, args=w_clause[1], fields=None, cnx=None, commit=True)

        except Exception as e:
            logging.error("RDBDataTable.delete_by_template_exception", exc_info=True)
            raise e

        return result

    def delete_by_key(self, key_fields):
        """
        :param key_fields: a list of key values in order as defined in key definition
        :return:
        """
        try:
            k = dict(zip(self._key_columns, key_fields))
            return self.delete_by_template(k)

        except Exception as e:
            logging.error("RDBDataTable.delete_by_key_exception", exc_info=True)
            raise e

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        # Get the list of columns.
        column_list = list(new_record.keys())

        # Get corresponding list of values.
        value_list = list(new_record.values())

        # Name of table.
        t_name = self._table_name

        # Perform insert.
        self._run_insert(t_name, column_list, value_list)

    def _key_to_template(self, key):
        result = dict(zip(self._key_columns, key))
        return result

    def update_by_template(self, template, new_values):
        """

        :param template: A template that defines which matching rows to update.
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records.
        :return: The number of rows updates.
        """
        result = None
        terms = []
        set_args = []

        try:

            for k, v in new_values.items():
                terms.append(k + "=%s")
                set_args.append(v)

            terms = ",".join(terms)

            w_clause = self._template_to_where_clause(template)

            set_args.extend(w_clause[1])

            q = "update " + self._table_name + " set " + str(terms) + " " + w_clause[0]

            result = self._run_q(q, set_args, fetch=False)

        except Exception as e:
            logging.error("RDBDataTable.update_by_template_exception", exc_info=True)
            raise e

        return result

    def update_by_key(self, key_fields, new_values):
        try:
            k = dict(zip(self._key_columns, key_fields))
            return self.update_by_template(k, new_values)

        except Exception as e:
            logging.error("RDBDataTable.update_by_key_exception", exc_info=True)
            raise e

    def find_by_related_key(self, key_fields, related_resource, tmp, field_list=None,
                        limit=None, offset=None, order_by=None, commit=True):

        foreign_key = self._get_foreign_key(related_resource)

        # Zipping together key_columns and passed fields produces a valid template
        tmp[foreign_key] = key_fields[0]

        # Call find_by_template. This returns a DerivedDataTable.
        result = related_resource.find_by_template(tmp, field_list)

        # For various reasons, we do not return a DerivedDataTable for find_by_primary_key().
        # We return the single row. This follows REST semantics.
        if result is not None:
            result = result.get_rows()

            if result is not None and len(result) > 0:
                result = result[0]
            else:
                result = None

        return result

    def insert_by_related_key(self, key, new_row, related_resource):

        tmp = dict(zip(self._key_columns, key))

        for k, v in tmp.items():
            new_row[k] = v

        result = related_resource.insert(new_row)

        return result



