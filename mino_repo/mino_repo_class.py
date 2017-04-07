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

    def __get__next_foreign_key__(self):
        prefix_foreign_key = '__FK__'
        if len(self.foreignKeys.keys()) > 0:
            __list__ = []
            for _l_ in self.foreignKeys.keys():
                __list__.append(_l_)
            __list__.sort(reverse=True)
            __last_key__ = __list__[0]
            foreign_key_next = __last_key__.replace(prefix_foreign_key, '')
            if foreign_key_next.isnumeric():
                foreign_key_next = int(foreign_key_next) + 1
                return prefix_foreign_key + str(foreign_key_next).zfill(2)
        foreign_key_next = 1
        return prefix_foreign_key + str(foreign_key_next).zfill(2)

    def __filter_one_dim__(self, filter_value, filter_field, negative=False, dim_fields_to_load=[]):
        filter_table_name = self.dim_fields[filter_field]
        filter_table = self.master_tables[filter_table_name]
        # We extract the foreign keys of the table
        # First we get the names
        #right now we only consider one index per table
        _foreign_key = filter_table.index.name
        # Now we get the submatrix that has the foreign keys as columns and the rows of the value we got
        if negative:
            index_filter = filter_table.loc[filter_table[filter_field] != filter_value].index
        else:
            index_filter = filter_table.loc[filter_table[filter_field] == filter_value].index
        filtered_list = []
        fact_fields_to_load_list = self.fact_fields

        if len(dim_fields_to_load) > 0:
            fact_fields_to_load_list.append(_foreign_key)

        fact_fields_to_load = chr(34) + '" , "'.join(fact_fields_to_load_list) + chr(34)
        result = self.fact_table.loc[
            self.fact_table[_foreign_key].map(lambda x: x in index_filter),
            eval(fact_fields_to_load)]
        result = result.join(filter_table[dim_fields_to_load], how='left')
        return result

    def filter_facts(self, filter_value, filter_field, fields_to_load=[]):
        if filter_field.__class__ == list:
            print('Function not yet avalaible, please filter a single dim')
        return self.__filter_one_dim__(filter_value, filter_field, dim_fields_to_load=fields_to_load)


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
        # now we make sure we have all fields in the attributesc
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

    def create_dim(self, fields_to_move, dim_table_name, debug=False):
        __subtable__ = self.fact_table.ix[:, fields_to_move]
        foreign_key = self.__get__next_foreign_key__()
        if isinstance(fields_to_move, list):
            __aux_unique_values = __subtable__.drop_duplicates()
            __aux_unique_values = __aux_unique_values.reset_index(drop=True)
            self.fact_table = pandas.merge(self.fact_table, __aux_unique_values, on=fields_to_move, how='left')
        else:
            # for  a single dim we use lists so that we can then use map (much faster than merge)
            __aux_unique_values = __subtable__.unique().tolist()
            self.fact_table[foreign_key] = self.fact_table[fields_to_move].map(lambda x: __aux_unique_values.index(x))
            __aux_unique_values = pandas.DataFrame(__aux_unique_values, columns=[fields_to_move])
        self.fact_table = self.fact_table.drop(fields_to_move, axis=1)
        __aux_unique_values[foreign_key] = __aux_unique_values.index.map(lambda x: int(x))
        __temp_dim_table__ = __aux_unique_values.set_index(foreign_key, drop=True)
        foreign_key = self.__get__next_foreign_key__()
        # Adding to lists
        # dim_table
        self.master_tables[dim_table_name] = __temp_dim_table__
        self.master_tables_names.append(dim_table_name)
        # foreign_key
        if len(self.foreignKeys) == 0:
            self.foreignKeys[foreign_key] = dim_table_name
        elif foreign_key in self.foreignKeys:
            self.foreignKeys[foreign_key].append(dim_table_name)
            print('There seems to be an issue with shared foreign keys')
        else:
            self.foreignKeys[foreign_key] = dim_table_name
        # relation with fields
        # field lists
        self.master_tree[dim_table_name] = {'parent':'','child':[]}

        if isinstance(fields_to_move, list):
            for _field_to_move in fields_to_move:
                self.dim_fields[_field_to_move] = dim_table_name
                self.fact_fields.remove(_field_to_move)
        else:
            self.dim_fields[fields_to_move] = dim_table_name
            self.fact_fields.remove(fields_to_move)

    def create_dim2(self, fields_to_move, dim_table_name):
        if isinstance(fields_to_move, list):
            dim_table_origin_name = self.dim_fields[fields_to_move[0]]
            for element in fields_to_move:
                if dim_table_origin_name != self.dim_fields[element]:
                    print('Error: Dimensions from multiple origins')
                    return False

            dim_table_origin = self.master_tables[dim_table_origin_name]
            __subtable__ = dim_table_origin.ix[:, fields_to_move]
            foreign_key = self.__get__next_foreign_key__()
            __aux_unique_values = __subtable__.drop_duplicates()
            __aux_unique_values = __aux_unique_values.reset_index(drop=True)
            __aux_unique_values[foreign_key] = __aux_unique_values.index.map(lambda x: int(x))

            result = dim_table_origin.join(__aux_unique_values[foreign_key],
                                           on=fields_to_move, how='left', lsuffix='__drop__l')
            _l_todrop = [i for i in result.columns if "__drop__" in i]
            if len(_l_todrop):
                result = result.drop(_l_todrop, axis=1)
        else:
            dim_table_origin_name = self.dim_fields[fields_to_move]
            dim_table_origin = self.master_tables[dim_table_origin_name]
            __subtable__ = dim_table_origin.ix[:, fields_to_move]
            __aux_unique_values = __subtable__.unique().tolist()

            dim_table_origin[foreign_key] = dim_table_origin[fields_to_move].map(lambda x: __aux_unique_values.index(x))
            __aux_unique_values = pandas.DataFrame(__aux_unique_values, columns=[fields_to_move])
            __aux_unique_values[foreign_key] = __aux_unique_values.index.map(lambda x: int(x))

            result = dim_table_origin

        result = result.drop(fields_to_move, axis=1)
        __aux_unique_values = __aux_unique_values.set_index(foreign_key)

        self.master_tables[dim_table_name] = __aux_unique_values
        self.master_tables[dim_table_origin_name] = result
        self.master_tables_names.append(dim_table_name)

        self.master_tree[dim_table_name] = {'parent': dim_table_origin_name, 'child': []}
        self.master_tree[dim_table_origin_name]['child'].append(dim_table_name)

        self.reshape_dims()


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