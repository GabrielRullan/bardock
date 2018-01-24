import pandas
import numpy


class WrongForeignKeyException(Exception):
    pass


class AlreadyUsedTableException(Exception):
    pass


class FieldNotFoundException(Exception):
    pass


class FieldFromMultipleOriginsException(Exception):
    pass


class MinoRepo():
    def __init__(self, fact_table):
        self._foreignKey_suffix = '__FK__'

        # table that contains the fact data
        self._fact_table = fact_table

        # fields from the dim tables and where are they {date : {dim_date}}
        self.dim_fields = {}

        # list of foreign keys
        self.foreignKeys = {}

        self.master_tables = {}
        self.master_tables_names = []

        self.tables = [fact_table]
        self.master_tree = {}

    #######################GETS N SETS
    @property
    def fact_table(self, get_all=False):
        if not get_all:
            return self._fact_table.head()
        else:
            return self._fact_table

    @property
    def ft(self):
        return self.fact_table(True)

    @property
    def fact_fields(self):
        return [x for x in self._fact_table.columns.tolist() if x.find(self._foreignKey_suffix) == -1]

    @property
    def ff(self):
        return self.fact_fields

    #######################Structure fix
    # This method is used to fix the foreign keys list and the dim_fields list
    def _redo_foreign_keys_and_dim_fields(self):
        # first we drop them all
        self.foreignKeys = {}
        self.dim_fields = {}
        # for each dim table we get all foreign keys
        for item in self.master_tables_names:
            d_table_name = item
            d_table = self.master_tables[d_table_name]
            keys = d_table.columns.tolist()

            for key in keys:
                # for each key found we add it to the list with the table where we found it
                if key.find(self._foreignKey_suffix) > -1:
                    if key in self.foreignKeys:
                        self.foreignKeys[key].append(d_table_name)
                    else:
                        l = [d_table_name]
                        self.foreignKeys[key] = l

                # if it's not a foreign key its a dim field
                else:
                    if key in self.dim_fields:
                        self.dim_fields[key].append(d_table_name)
                    else:
                        l = [d_table_name]
                        self.dim_fields[key] = l

    # This method is used to fix the dim table list
    def __redo_dim_table(self):
        list_names = self.master_tables_names
        list_tables = self.master_tables.keys()
        for name in list_names:
            if name not in list_tables:
                print('We lack a table in the dictionary of master tables')
        for table in list_tables:
            if table not in list_names:
                self.master_tables_names.append(table)

    # This fixes the current structure
    def _fix_structure(self):
        self.__redo_dim_table()
        self._redo_foreign_keys_and_dim_fields()
        return True

    # This tells if the structure has been changed from outside
    def check_structure(self):
        __test_duplicates = []

        for __temp_dim_table in self.master_tables.keys():
            __test_duplicates += self.master_tables[__temp_dim_table].columns.tolist()
            __test_duplicates += [self.master_tables[__temp_dim_table].index.name]
        for i in __test_duplicates:
            if __test_duplicates.count(i) > 1:
                print('there are duplicated fields')
                print('please run a fix_structure()')
                return True

        # now we make sure we have all fields in the attributes
        # we do not include the foreign keys
        __right_dim_fields = [x for x in __test_duplicates if x.find(self._foreignKey_suffix) == -1]
        __current_dim_fields = list(self.dim_fields.keys())
        # we sort them
        __right_dim_fields.sort()
        __current_dim_fields.sort()
        # now compare them
        if not __right_dim_fields == __current_dim_fields:
            print('current structure is')
            print(__right_dim_fields)
            print('according to the attributes the structure should be')
            print(__current_dim_fields)
            print('please run a _fix_structure()')
            return True
        return False

    #################Creation of dim tables

    # This returns the next foreign key available
    def __get__next_foreign_key__(self):
        # if we have already keys, we need to sort tha last one out
        # if we do not have keys, we just return the first one
        key_list = self.foreignKeys.keys()
        last_key = 0
        if len(key_list) > 0:
            key_list = [int(x.replace(self._foreignKey_suffix, '')) for x in key_list]
            key_list.sort(reverse=True)
            last_key = key_list[0]
        foreign_key_next = int(last_key) + 1
        return self._foreignKey_suffix + str(foreign_key_next).zfill(3)

    # This method updates the lists of the object
    def __update_new_dim(self, dim_table_name, dim_table, foreign_key, fields_to_move, secondary=False):
        # update the master tables
        self.master_tables[dim_table_name] = dim_table
        self.master_tables_names.append(dim_table_name)
        self.master_tree[dim_table_name] = {'parent': '', 'child': []}

        # update foreign_keys
        if foreign_key not in self.foreignKeys:
            l = [dim_table_name]
            self.foreignKeys[foreign_key] = l
        else:
            self.foreignKeys[foreign_key].append(dim_table_name)

        if not secondary:
            # fact fields
            if isinstance(fields_to_move, list):
                for field in fields_to_move:
                    self.dim_fields[field] = dim_table_name
            else:
                self.dim_fields[fields_to_move] = dim_table_name
        else:
            # dim fields moved
            if isinstance(fields_to_move, list):
                for field in fields_to_move:
                    self.dim_fields[field] = dim_table_name
            else:
                self.dim_fields[fields_to_move] = dim_table_name

    # Given a field name and a new table name this takes out the field and create a new table
    def create_dim(self, fields_to_move, dim_table_name, debug=False, secondary=False):
        if not secondary:
            table_origin = self._fact_table
        else:
            dim_table_origin_name = self.dim_fields[fields_to_move[0]]
            table_origin = self.master_tables[dim_table_origin_name]
        # checks
        # Is the name already in use?
        if dim_table_name in self.master_tables_names:
            raise AlreadyUsedTableException
        # Is the field or fields available?
        if isinstance(fields_to_move, list):
            for field in fields_to_move:
                if field not in table_origin.columns:
                    raise FieldNotFoundException
        else:
            if fields_to_move not in table_origin.columns:
                raise FieldNotFoundException

        # we get the table to be substracted
        subtable = table_origin.ix[:, fields_to_move]
        foreign_key = self.__get__next_foreign_key__()

        # if we are dealing with more than one field then we use unique from pandas
        if isinstance(fields_to_move, list):
            dim_table = subtable.drop_duplicates()
            # we'll use the index as the foreign key value
            dim_table = dim_table.reset_index(drop=True)
            # we add the foreign key
            dim_table[foreign_key] = dim_table.index
            # we add the index/future foreign key as a value to the fact table
            table_origin = pandas.merge(table_origin, dim_table, on=fields_to_move, how='left')
            dim_table = dim_table.set_index(foreign_key, drop=True)
        else:
            # for  a single dim we use lists so that we can then use map (much faster than merge)
            dim_table_list = subtable.unique().tolist()
            # we use the list index as the foreign key
            table_origin[foreign_key] = table_origin.map(lambda x: dim_table_list.index(x))
            # we transform the list into a data frame
            dim_table = pandas.DataFrame(dim_table_list, columns=[fields_to_move])
            # we add the foreign key
            dim_table[foreign_key] = dim_table.index
            dim_table = dim_table.set_index(foreign_key, drop=True)

        table_origin = table_origin.drop(fields_to_move, axis=1)
        if not secondary:
            self._fact_table = table_origin
        else:
            dim_table_origin_name = self.dim_fields[fields_to_move[0]]
            self.master_tables[dim_table_origin_name] = table_origin
        # Adding to lists
        self.__update_new_dim(dim_table_name, dim_table, foreign_key, fields_to_move, secondary)

    def full_thread(self, dim_table_name):
        thread_down = self.thread_to_fact(dim_table_name, self.master_tables[dim_table_name])
        ####pending
        thread_up = [{'key': 1, 'table_name': 'a'}]

    def thread_to_fact(self, dim_table_name, dim_table):
        foreign_key = dim_table.index.name
        fields = self.fact_table.columns.tolist()
        keys = [x for x in fields if x.find(self._foreignKey_suffix) > -1]
        if foreign_key in keys:
            # If the foreign key is on the fact and the table, distance is 1
            thread = [{'key': foreign_key, 'table_name': dim_table_name}]
            return thread
        # we need to repeat the process until we get to the table
        level = 1
        thread = [{'key': foreign_key, 'table_name': dim_table_name, 'level': level}]
        tables = self.foreignKeys[foreign_key].copy()
        print(tables)
        tables = tables.remove(dim_table_name)
        print(tables)
        thread_append = self.thread_to_fact_recursive(tables, keys, level)

        return thread.append(thread_append)

    def thread_to_fact_recursive(self, tables, keys_in_fact_table, level):
        level_now = level + 1
        thread_append = {}
        for dim_table_name in tables:
            foreign_key = dim_table_name.index.name
            fields = self.fact_table.columns.tolist()
            if foreign_key in keys_in_fact_table:
                # If the foreign key is on the fact and the table, distance is 1
                thread_append = {'key': foreign_key, 'table_name': dim_table_name, 'level': level_now}
                return thread_append
            # we are only interested if the table is connected to other tables
            elif len(self.foreignKeys[foreign_key]) > 1:
                tables_now = self.foreignKeys(foreign_key).copy()
                tables_now = tables_now.remove(dim_table_name)
                thread_append = self.thread_to_fact_recursive(tables_now, keys_in_fact_table, level_now)
                # if you have thread it means you found the fact table
                if len(thread_append) > 0:
                    thread_append = thread_append.append({'key': foreign_key, 'table_name': dim_table_name,
                                                          'level': level_now})
                    return thread_append
        # if we didn't find anything we return an empty thread
        return thread_append

    ############FILTERING
    def __filter_one_dim__(self, filter_value, filter_field, negative=False, dim_fields_to_load=[]):
        table_origin_name = self.dim_fields[filter_field]
        table_origin = self.master_tables[table_origin_name]
        _foreign_key = table_origin.index.name
        thread = self.thread_to_fact(table_origin_name, table_origin)

        filter_table = self.master_tables[table_origin_name]
        filter_table = filter_table[filter_field]
        # We extract the foreign keys of the table
        # First we get the names
        # right now we only consider one index per table
        _foreign_key = filter_table.index.name
        # Now we get the submatrix that has the foreign keys as columns and the rows of the value we got
        index_filter = numpy.nonzero(filter_table == filter_value)[0]
        if negative:
            index_filter = filter_table.index.drop(index_filter)
        fact_fields_to_load_list = self.fact_fields

        if len(dim_fields_to_load) > 0:
            fact_fields_to_load_list.append(_foreign_key)
            tables = set([self.dim_fields[x] for x in dim_fields_to_load])
            tables_d = {}
            for i in tables:
                tables_d[i] = [x for x in dim_fields_to_load if self.dim_fields[x] == i]

        fact_fields_to_load = chr(34) + '" , "'.join(fact_fields_to_load_list) + chr(34)
        fact_index_to_filter = numpy.isin(self._fact_table[_foreign_key], index_filter)
        result = self._fact_table.loc[fact_index_to_filter, eval(fact_fields_to_load)]
        if len(dim_fields_to_load) > 0:
            print('adding dim columns is not yet well implemented')
            for table_name in tables_d.keys():
                table = self.master_tables[table_name]
                fields_table = tables_d[table_name]
                result = result.join(table[fields_table], on=table.index.name)
            # result = result.join(filter_table[dim_fields_to_load], how='left')
        return result

    def filter_facts(self, filter_value, filter_field, fields_to_load=[], negative = False):
        if filter_field.__class__ == list:
            print('Function not yet avalaible, please filter a single dim')
        return self.__filter_one_dim__(filter_value, filter_field,  negative, dim_fields_to_load=fields_to_load)

    #####################SUMMARY
    @property
    def summary(self):
        print('*fact_columns:  ')
        print(chr(9) + ' , '.join(self.fact_fields))
        _rowN = str(self._fact_table.count()[1])
        print(chr(9) + 'rows: ' + _rowN)

        print('*dim_fields:  ')
        print(chr(9) + ' , '.join(self.dim_fields))

        print('*foreign Keys:  ')
        _s2 = []
        for k in self.foreignKeys.keys():
            for t in self.foreignKeys[k]:
                print(chr(9) + k + ' -> ' + t)

        print('*master_tables_names:  ')
        print(chr(9) + ' ; '.join(self.master_tables_names))

    @property
    def sample(self):
        print('this is a sample of values')
        for field in self.dim_fields.keys():
            table_name = self.dim_fields[field]
            value = self.master_tables[table_name].loc[0, field]
            s = ' \t {} sample \t {} '.format(field, value)
            print(s)
