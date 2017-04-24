import pandas


class WrongForeignKeyException(Exception):
    pass
class AlreadyUsedTableException(Exception):
    pass
class FieldNotFoundException(Exception):
    pass

class MinoRepo():
    def __init__(self, fact_table):
        self.foreignKey_suffix = '__FK__'

        #table that contains the fact data
        self._fact_table = fact_table
        self.fact_fields = fact_table.columns.tolist()

        #fields from the dim tables and where are they {date : {dim_date}}
        self.dim_fields = {}

        #list of foreign keys
        self.foreignKeys = {}

        self.master_tables = {}
        self.master_tables_names = []

        self.tables = [fact_table]
        self.master_tree = {}

#######################GETS N SETS
    @property
    def fact_table(self, get_all = False):
        if get_all == False:
            return self._fact_table.head()
        else:
            return  self._fact_table
    @property
    def ft(self):
        return self.fact_table(True)

    @property
    def ff(self):
        return self.fact_fields

#######################Structure fix
    # This method is used to fix the foreign keys list
    def __redo_foreignkeys(self):
        self.foreignKeys = {}
        #for each dim table we get all foreign keys
        for d_table_name in self.master_tables_names:
            d_table = self.master_tables[d_table_name]
            keys = d_table.columns.tolist()
            for key in keys:
                #for each key found we add it to the list with the table where we found it
                if key.find(self.foreignKey_suffix) == 0:
                    if key in self.foreignKeys:
                        self.foreignKeys[key].append(d_table_name)
                    else:
                        l = d_table_name
                        self.foreignKeys[key] = l

    # This method is used to fix the fact fields list
    def _redo_fact_fields(self):
        self.fact_fields = []
        #we get the fields from the fact table and substract the ones that are foreign keys
        fields = self._fact_table.columns()
        for field in fields:
            if self.foreignKey_suffix in field:
                self.fact_fields.append(field)

    # This method is used to fix the fields list
    def __redo_dim_fields(self):
        self.dim_fields = {}
        for dim_table_name in self.master_tables_names:
            dim_table = self.master_tables[dim_table_name]
            columns_list = dim_table.columns.tolist()
            for column_name in columns_list:
                self.dim_fields[column_name] = dim_table_name

    # This method is used to fix the dim table list
    def __redo_dim_table(self):
        list_names = self.master_tables_names
        list_tables = self.master_tables.keys()
        for name in list_names:
            if not name in list_tables:
                print('We lack a table in the dictionary of master tables')
        for table in list_tables:
            if not table in list_names:
                self.master_tables_names.append(table)

    #This fixes the current structure
    def fix_structure(self):
        self.__redo_dim_table()
        self.__redo_foreignkeys()
        self.__redo_dim_fields()
        self.__redo_fact_fields()
        return True

    #This tells if the structure has been changed from outside
    def find_wrong_stucture(self):
        __test_duplicates = []
        __right_dim_fields = []
        for __temp_dim_table in self.master_tables.keys():
            __test_duplicates += self.master_tables[__temp_dim_table].columns.tolist()
            __test_duplicates += [self.master_tables[__temp_dim_table].index.name]
        for i in __test_duplicates:
            if __test_duplicates.count(i) > 1:
                print('there are duplicated fields')
                print('please run a fix_structure()')
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
            print('please run a fix_structure()')
            return True
        return False

#################Creation of dim tables

    #This returns the next foreign key avalaible
    def __get__next_foreign_key__(self):
        self.__redo_foreignkeys()
        # if we have already keys, we need to sort tha last one out
        # if we do not have keys, we just return the first one
        if len(self.foreignKeys.keys()) == 0:
            __last_key__ = 0
        else:
            __list__ = []
            for _l_ in self.foreignKeys.keys():
                __list__.append(_l_)
            __list__.sort(reverse=True)
            __last_key__ = __list__[0].replace(self.foreignKey_suffix, '')
            if not __last_key__.isnumeric():
                raise WrongForeignKeyException
        foreign_key_next = int(__last_key__) + 1
        return self.foreignKey_suffix + str(foreign_key_next).zfill(3)

    # This method updates the lists of the object
    def __update_new_dim(self, dim_table_name, dim_table, foreign_key, fields_to_move):
        # update the master tables
        self.master_tables[dim_table_name] = dim_table
        self.master_tables_names.append(dim_table_name)
        self.master_tree[dim_table_name] = {'parent': '', 'child': []}

        # update foreign_keys
        if len(self.foreignKeys) == 0:
            l = []
            l.append(dim_table_name)
            self.foreignKeys[foreign_key] = l
        elif foreign_key in self.foreignKeys:
            self.foreignKeys[foreign_key].append(dim_table_name)
        else:
            l = []
            l.append(dim_table_name)
            self.foreignKeys[foreign_key] = l

        #fact fields
        if isinstance(fields_to_move, list):
            for field in fields_to_move:
                self.dim_fields[field] = dim_table_name
                self.fact_fields.remove(field)
        else:
            self.dim_fields[fields_to_move] = dim_table_name
            self.fact_fields.remove(fields_to_move)


    # Given a field name and a new table name this takes out the field and create a new table
    def create_dim(self, fields_to_move, dim_table_name, debug=False):
        #checkings
            #Is the name already in use?
        if dim_table_name in self.master_tables_names:
            raise AlreadyUsedTableException
            #Is the field or fields avalaible?
        if isinstance(fields_to_move, list):
            for field in fields_to_move:
                if not field in self._fact_table.columns:
                    raise FieldNotFoundException
        else:
            if not fields_to_move in self._fact_table.columns:
                raise FieldNotFoundException

        # we get the table to be substracted
        subtable = self._fact_table.ix[:, fields_to_move]
        foreign_key = self.__get__next_foreign_key__()

        # if we are dealing with more than one field then we use unique from pandas
        if isinstance(fields_to_move, list):
            dim_table = subtable.drop_duplicates()
            # we'll use the index as the foreign key value
            dim_table = dim_table.reset_index(drop=True)
            # we add the foreign key
            dim_table[foreign_key] = dim_table.index
            dim_table = dim_table.set_index(foreign_key, drop=True)
            # we add the index/future foreign key as a value to the fact table
            self._fact_table = pandas.merge(self._fact_table, dim_table, on=fields_to_move, how='left')
        else:
            # for  a single dim we use lists so that we can then use map (much faster than merge)
            dim_table_list = subtable.unique().tolist()
            # we use the list index as the foreign key
            self._fact_table[foreign_key] = self._fact_table[fields_to_move].map(lambda x: dim_table_list.index(x))
            # we transform the list into a data frame
            dim_table = pandas.DataFrame(dim_table_list, columns=[fields_to_move])
            # we add the foreign key
            dim_table[foreign_key] = dim_table.index
            dim_table = dim_table.set_index(foreign_key, drop=True)

        self._fact_table = self._fact_table.drop(fields_to_move, axis=1)
        # Adding to lists
        self.__update_new_dim(dim_table_name, dim_table, foreign_key, fields_to_move)

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


############FILTERING
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
        result = self._fact_table.loc[
            self._fact_table[_foreign_key].map(lambda x: x in index_filter),
            eval(fact_fields_to_load)]
        result = result.join(filter_table[dim_fields_to_load], how='left')
        return result

    def filter_facts(self, filter_value, filter_field, fields_to_load=[]):
        if filter_field.__class__ == list:
            print('Function not yet avalaible, please filter a single dim')
        return self.__filter_one_dim__(filter_value, filter_field, dim_fields_to_load=fields_to_load)


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
            print(chr(9) + k + ' -> ' + self.foreignKeys[k])

        print('*master_tables_names:  ')
        print(chr(9) + ' ; '.join(self.master_tables_names))
    @property
    def sample(self):
        print('this is a sample of values')
        for field in self.dim_fields.keys():
            table_name = self.dim_fields[field]
            value = self.master_tables[table_name].loc[0,field]
            s = ' \t {} sample \t {} '.format(field, value)
            print(s)