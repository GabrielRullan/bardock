import pandas

class MinoRepo():
    def __init__(self, fact_table):
        self.fact_table = fact_table
        self.fields = fact_table.columns.tolist()
        self.fact_fields = fact_table.columns.tolist()
        self.dim_fields = {}
        self.foreignKeys = {}
        self.tables = [fact_table]
        self.master_tables = {}
        self.master_tables_names = []

    def find_wrong_stucture(self):
        __test_duplicates = []
        __right_dim_fields = []
        for __temp_dim_table in self.master_tables.keys():
            __test_duplicates += self.master_tables[__temp_dim_table].columns.tolist()
            __test_duplicates += [self.master_tables[__temp_dim_table].index.name]
        for i in __test_duplicates:
            if __test_duplicates.count(i) > 1:
                print('there are duplicated fields')
                print('please run a reshape_dims()')
                return True
        # now we make sure we have all fields in the attributes
        # we do not include the foreign keys
        __right_dim_fields = [x for x in __test_duplicates if x.find('__FK__') == -1]
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
            print('please run a reshape_dims()')
            return True
        return False

    def reshape_dims(self):
        self.dim_fields = {}

        for __temp_dim_table_name in self.master_tables_names:
            __temp_dim_table = self.master_tables[__temp_dim_table_name]
            __columns_list = __temp_dim_table.columns.tolist()
            for __column_name in __columns_list:
                self.dim_fields[__column_name] = __temp_dim_table_name

    def create_dim(self, fields_to_move, dim_table_name, debug=True):
        if debug: print(0)
        __subtable__ = self.fact_table.ix[:, fields_to_move]
        foreing_key = self.__get__next_foreign_key__()
        if isinstance(fields_to_move, list):
            __aux_unique_values = __subtable__.drop_duplicates()
            if debug: print(0)
            __aux_unique_values = __aux_unique_values.reset_index(drop=True)
            self.fact_table = pandas.merge(self.fact_table, __aux_unique_values, on=fields_to_move, how='left')
        else:
            # for  a single dim we use lists so that we can then use map (much faster than merge)
            __aux_unique_values = __subtable__.unique().tolist()
            if debug: print(0)
            self.fact_table[foreing_key] = self.fact_table[fields_to_move].map(lambda x: __aux_unique_values.index(x))
            __aux_unique_values = pandas.DataFrame(__aux_unique_values, columns=[fields_to_move])
        if debug: print(0)
        self.fact_table = self.fact_table.drop(fields_to_move, axis=1)
        __aux_unique_values[foreing_key] = __aux_unique_values.index.map(lambda x: int(x))
        __temp_dim_table__ = __aux_unique_values.set_index(foreing_key, drop=True)
        foreing_key = self.__get__next_foreign_key__()
        if debug: print(0)
        # Adding to lists
        # dim_table
        self.master_tables[dim_table_name] = __temp_dim_table__
        self.master_tables_names.append(dim_table_name)
        # foreign_key
        if debug: print(0)
        if len(self.foreignKeys) == 0:
            self.foreignKeys[foreing_key] = dim_table_name
        elif foreing_key in self.foreignKeys:
            self.foreignKeys[foreing_key].append(dim_table_name)
            print('There seems to be an issue with shared foreign keys')
        else:
            self.foreignKeys[foreing_key] = dim_table_name
        # relation with fields
        # field lists
        if isinstance(fields_to_move, list):
            for _field_to_move in fields_to_move:
                self.dim_fields[_field_to_move] = dim_table_name
                self.fact_fields.remove(_field_to_move)
        else:
            self.dim_fields[fields_to_move] = dim_table_name
            self.fact_fields.remove(fields_to_move)

    def __get__next_foreign_key__(self):
        prefix_foreing_key = '__FK__'
        if len(self.foreignKeys.keys()) > 0:
            __list__ = []
            for _l_ in self.foreignKeys.keys():
                __list__.append(_l_)
            __list__.sort(reverse=True)
            __last_key__ = __list__[0]
            foreing_key_next = __last_key__.replace(prefix_foreing_key, '')
            if foreing_key_next.isnumeric():
                foreing_key_next = int(foreing_key_next) + 1
                return prefix_foreing_key + str(foreing_key_next).zfill(2)
        foreing_key_next = 1
        return prefix_foreing_key + str(foreing_key_next).zfill(2)

    def filter_facts(self, filter_value, filter_field):
        if filter_field.__class__ == list:
            print('Function not yet avalaible, please filter a single dim')
        return self.__filter_one_dim__(filter_value, filter_field)

    def __filter_one_dim__(self, filter_value, filter_field, negative=False):
        filter_table_name = self.dim_fields[filter_field]
        filter_table = self.master_tables[filter_table_name]
        # We extract the foreign keys of the table
        # First we get the names
        #######right now we only consider one index per table
        _foreing_key = filter_table.index.name
        # Now we get the submatix that has the foreign keys as columns and the rows of the value we got
        if negative:
            index_filter = filter_table.loc[filter_table[filter_field] != filter_value].index
        else:
            index_filter = filter_table.loc[filter_table[filter_field] == filter_value].index
        filtered_list = []
        fields_to_load = chr(34) + '" , "'.join(self.fact_fields) + chr(34)
        if True:
            return self.fact_table.loc[
                self.fact_table[_foreing_key].map(lambda x: x in index_filter), eval(fields_to_load)]

        # esto es para que en un futuro se filtren multiples foreign
        _foreing_keys_list = []
        for _idx in index_filter:
            __row = {}
            for __colName in _foreing_keys_list:
                __row[__colName] = self.master_tables[filter_table_name].ix[_idx, __colName]
            filtered_list.append(__row)
        # Now we create the query
        filter_query_list = []
        for __row in filtered_list:
            query_as_list = []

            for __colName in __row.keys():
                column_query = 'self.fact_table["' + __colName + '"] == {0}'.format(str(__row[__colName]))
                query_as_list.append(column_query)

            filter_query = '(' + ') && ('.join(query_as_list) + ')'
            filter_query_list.append(filter_query)

        final_filter_query = '(' + ' | '.join(filter_query_list) + ')'

        fields_to_load = chr(34) + '" , "'.join(self.fact_fields) + chr(34)
        return self.fact_table.loc[eval(final_filter_query), eval(fields_to_load)]

    def summary(self):
        print('*fact_columns:  ')
        print(chr(9) + ' , '.join(self.fact_fields))
        _rowN = str(self.fact_table.count()[1])
        print(chr(9) + 'rows: ' + _rowN)

        print('*dim_fields:  ')
        print(chr(9) + ' , '.join(self.dim_fields))

        print('*foreign Keys:  ')
        _s2 = []
        for k in self.foreignKeys.keys():
            print(chr(9) + k + ' -> ' + self.foreignKeys[k])

        print('*master_tables_names:  ')
        print(chr(9) + ' ; '.join(self.master_tables_names))