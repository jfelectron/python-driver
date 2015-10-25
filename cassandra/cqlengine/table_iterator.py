class TableIterator(object):
    """
    Iterates over a Cassandra table defined by a cqlengine model class using query paging in order to pull back chunks
    of data that have `blocksize` number of records.
    Can optionally provide kwargs which are used as where clause filters. These kwargs must be columns on the model
    which are indexed.
    :param model_class: The cqlengine model object that defines the table you want to iterate over.
    :type model_class: An instance of cqlengine.Model.
    :param blocksize: The number of results you want to pull back with each paging query. Can be used to tune the
                        performance of the iteration depending on the characteristics of the table.
    :type blocksize: integer
    :param where_filters: Keyword arguments can be passed to the iterator and these will be used as filter parameters
                        when querying the database for pages of data.
    :type where_filters: **kwargs
    :return: an iterator over the a collection of objects of type model_class.
    """

    def __init__(self, model_class, blocksize=10000, **where_filters):
        self.model_class = model_class

        # Pull the keys of the model class for convenient reference.
        self.partition_keys = model_class._partition_keys
        self.clustering_keys = model_class._clustering_keys
        self.blocksize = blocksize
        self.where_filters = where_filters

    @classmethod
    def generate_where_clause_key(cls, column_name, clause_condition):
        """
        Takes the name of a primary key column and a condition ('gt', 'lt', 'eq') and creates a where clause key that
        can be used with a cqlengine .objects() descriptor to filter based on that condition.
        :param key_name: The name of the model column (primary key) for which you want to generate a where clause.
        :type key_name: The string representation of the column name.
        :param clause_condition: The conditional operator you want to filter by.
        :type clause_condition: A string that is a valid cqlengine conditional operator ('gt', 'lt', 'eq')
        :return: A string of the form "{my_column_name}__{my_clause_condition}".
        """
        return "{}__{}".format(column_name, clause_condition)

    @classmethod
    def get_paging_where_clause_key(cls, primary_key_column):
        """
        Get a where clause key value that can be used to page through the primary key column.
        :param primary_key_column: A primary key column class you want a key to page over.
        :type primary_key_column:  An class that inherits from cqlengine.Column.
        :return: A string of the format "{column_name}__{where_condition}" which will page data from that column in the
                direction defined by the clustering order of that column.
                For example, if I have a clustering key named `my_cluster_key` which has a descending clustering order,
                this function will return the key 'my_cluster_key__lt' which can be used to page over the value of
                my_cluster_key in it's default order.
        """
        condition_lookup = {"asc": "gt", "desc": "lt"}
        clustering_order = getattr(primary_key_column, "clustering_order") or "asc"
        clause_condition = condition_lookup[clustering_order.lower()]
        return cls.generate_where_clause_key(primary_key_column.column_name, clause_condition)

    def get_next_query_set(self, previous_object):
        """
        Takes a cqlengine model object instance and treats that object as the current cursor into a Cassandra table
        generating a cqlengine query object which will page in the set of results immediately following
        `previous_object` according to Cassandra partition tokens and clustering order.
        :param previous_object: The last object fetched by the previous paging query. Can also be viewed as the cursor
                    location for this table iteration.
        :type previous_object: A cqlengine model object instance.
        :return: A cqlengine QuerySet object that will return the objects immediately following `previous_object` in the
                Cassandra table.
        """
        prev_partition_key_vals = {}
        prev_clustering_key_vals = {}

        # Pull all of the key values off of previous_object
        for p_key_name, _ in self.partition_keys.items():
            prev_partition_key_vals[p_key_name] = getattr(previous_object, p_key_name)
        for c_key_name, _ in self.clustering_keys.items():
            prev_clustering_key_vals[c_key_name] = getattr(previous_object, c_key_name)

        # Copy the clustering keys dict since we want to use the values it contains as **kwargs to a QuerySet. We need
        # to alter the values without clobbering the original values.
        cluster_where_clause = dict(prev_clustering_key_vals.items())

        # Iterator over the ordered clustering keys in reverse order.
        for c_key_name, c_key_col in reversed(self.clustering_keys.items()):
            # Drop the equals clause for the current clustering key because we want a paging conditional ('gt' or 'lt').
            del cluster_where_clause[c_key_name]

            # Generate a paging clause for this clustering key that we will use as a where clause filter.
            new_where_key = self.get_paging_where_clause_key(c_key_col)
            cluster_where_clause[new_where_key] = prev_clustering_key_vals[c_key_name]

            # Generate our new where clause consisting of the current partition, our paging clustering conditions and
            # any where_filters that were originally handed to TableIterator.
            where_clause = dict(prev_partition_key_vals.items() + cluster_where_clause.items() + self.where_filters.items())

            current_query = self.model_class.objects(**where_clause).limit(self.blocksize)

            # TODO: Can we optimize to return results from this function rather than doing garbage query round trip?
            if current_query.first():
                # This query returns objects, so it's a valid page and we want to use it.
                return current_query
            else:
                # This query returned nothing so we have exhausted the clustering key we are currently looking at.
                # Drop the clause for this clustering key and continue to the next one.
                del cluster_where_clause[new_where_key]

        # We made it through testing all of the clustering key values and got no results so we have exhausted the
        # current partition we are looking at.

        # Generate the partition key token for the last seen object.
        token = cqlengine.Token(previous_object.pk)

        # Create a where clause for the partition key token.
        pk_token_where = self.generate_where_clause_key('pk__token', 'gt')
        partition_key_clause = {pk_token_where: token}

        where_clause = dict(partition_key_clause.items() + self.where_filters.items())

        query = self.model_class.objects(**where_clause).limit(self.blocksize)

        return query

    def __iter__(self):
        """
        Returns an iterator over the objects that exist in the table passed into __init__.
        """

        done_iterating = False
        query = self.model_class.objects(**self.where_filters).limit(self.blocksize)

        while not done_iterating:
            previous_object = None

            for obj in query:
                previous_object = obj
                yield obj

            if not previous_object is None:
                query = self.get_next_query_set(previous_object)
            else:
                done_iterating = True
