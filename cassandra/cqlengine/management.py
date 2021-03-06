# Copyright 2015 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import namedtuple
import json
import logging
import os
import six
import warnings

from cassandra import metadata
from cassandra.cqlengine import CQLEngineException
from cassandra.cqlengine import columns
from cassandra.cqlengine.connection import execute, get_cluster
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.named import NamedTable
from cassandra.cqlengine.usertype import UserType

CQLENG_ALLOW_SCHEMA_MANAGEMENT = 'CQLENG_ALLOW_SCHEMA_MANAGEMENT'

Field = namedtuple('Field', ['name', 'type'])

log = logging.getLogger(__name__)

# system keyspaces
schema_columnfamilies = NamedTable('system', 'schema_columnfamilies')


def create_keyspace_simple(name, replication_factor, durable_writes=True):
    """
    Creates a keyspace with SimpleStrategy for replica placement

    If the keyspace already exists, it will not be modified.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*

    :param str name: name of keyspace to create
    :param int replication_factor: keyspace replication factor, used with :attr:`~.SimpleStrategy`
    :param bool durable_writes: Write log is bypassed if set to False
    """
    _create_keyspace(name, durable_writes, 'SimpleStrategy',
                     {'replication_factor': replication_factor})


def create_keyspace_network_topology(name, dc_replication_map, durable_writes=True):
    """
    Creates a keyspace with NetworkTopologyStrategy for replica placement

    If the keyspace already exists, it will not be modified.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*

    :param str name: name of keyspace to create
    :param dict dc_replication_map: map of dc_names: replication_factor
    :param bool durable_writes: Write log is bypassed if set to False
    """
    _create_keyspace(name, durable_writes, 'NetworkTopologyStrategy', dc_replication_map)


def _create_keyspace(name, durable_writes, strategy_class, strategy_options):
    if not _allow_schema_modification():
        return

    cluster = get_cluster()

    if name not in cluster.metadata.keyspaces:
        log.info("Creating keyspace %s ", name)
        ks_meta = metadata.KeyspaceMetadata(name, durable_writes, strategy_class, strategy_options)
        execute(ks_meta.as_cql_query())
    else:
        log.info("Not creating keyspace %s because it already exists", name)


def drop_keyspace(name):
    """
    Drops a keyspace, if it exists.

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    :param str name: name of keyspace to drop
    """
    if not _allow_schema_modification():
        return

    cluster = get_cluster()
    if name in cluster.metadata.keyspaces:
        execute("DROP KEYSPACE {0}".format(metadata.protect_name(name)))


def sync_table(model):
    """
    Inspects the model and creates / updates the corresponding table and columns.

    Any User Defined Types used in the table are implicitly synchronized.

    This function can only add fields that are not part of the primary key.

    Note that the attributes removed from the model are not deleted on the database.
    They become effectively ignored by (will not show up on) the model.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*
    """
    if not _allow_schema_modification():
        return

    if not issubclass(model, Model):
        raise CQLEngineException("Models must be derived from base Model.")

    if model.__abstract__:
        raise CQLEngineException("cannot create table from abstract model")

    cf_name = model.column_family_name()
    raw_cf_name = model._raw_column_family_name()

    ks_name = model._get_keyspace()

    cluster = get_cluster()

    keyspace = cluster.metadata.keyspaces[ks_name]
    tables = keyspace.tables

    syncd_types = set()
    for col in model._columns.values():
        udts = []
        columns.resolve_udts(col, udts)
        for udt in [u for u in udts if u not in syncd_types]:
            _sync_type(ks_name, udt, syncd_types)

    # check for an existing column family
    if raw_cf_name not in tables:
        log.debug("sync_table creating new table %s", cf_name)
        qs = _get_create_table(model)

        try:
            execute(qs)
        except CQLEngineException as ex:
            # 1.2 doesn't return cf names, so we have to examine the exception
            # and ignore if it says the column family already exists
            if "Cannot add already existing column family" not in unicode(ex):
                raise
    else:
        log.debug("sync_table checking existing table %s", cf_name)
        # see if we're missing any columns
        field_names = _get_non_pk_field_names(tables[raw_cf_name])
        model_fields = set()
        # # TODO: does this work with db_name??
        for name, col in model._columns.items():
            if col.primary_key or col.partition_key:
                continue  # we can't mess with the PK
            model_fields.add(name)
            if col.db_field_name in field_names:
                continue  # skip columns already defined

            # add missing column using the column def
            query = "ALTER TABLE {0} add {1}".format(cf_name, col.get_column_def())
            execute(query)

        db_fields_not_in_model = model_fields.symmetric_difference(field_names)
        if db_fields_not_in_model:
            log.info("Table %s has fields not referenced by model: %s", cf_name, db_fields_not_in_model)

        _update_options(model)

    table = cluster.metadata.keyspaces[ks_name].tables[raw_cf_name]

    indexes = [c for n, c in model._columns.items() if c.index]

    # TODO: support multiple indexes in C* 3.0+
    for column in indexes:
        index_name = 'index_{0}_{1}'.format(raw_cf_name, column.db_field_name)
        if table.indexes.get(index_name):
            continue

        qs = ['CREATE INDEX {0}'.format(index_name)]
        qs += ['ON {0}'.format(cf_name)]
        qs += ['("{0}")'.format(column.db_field_name)]
        qs = ' '.join(qs)
        execute(qs)


def sync_type(ks_name, type_model):
    """
    Inspects the type_model and creates / updates the corresponding type.

    Note that the attributes removed from the type_model are not deleted on the database (this operation is not supported).
    They become effectively ignored by (will not show up on) the type_model.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*
    """
    if not _allow_schema_modification():
        return

    if not issubclass(type_model, UserType):
        raise CQLEngineException("Types must be derived from base UserType.")

    _sync_type(ks_name, type_model)


def _sync_type(ks_name, type_model, omit_subtypes=None):

    syncd_sub_types = omit_subtypes or set()
    for field in type_model._fields.values():
        udts = []
        columns.resolve_udts(field, udts)
        for udt in [u for u in udts if u not in syncd_sub_types]:
            _sync_type(ks_name, udt, syncd_sub_types)
            syncd_sub_types.add(udt)

    type_name = type_model.type_name()
    type_name_qualified = "%s.%s" % (ks_name, type_name)

    cluster = get_cluster()

    keyspace = cluster.metadata.keyspaces[ks_name]
    defined_types = keyspace.user_types

    if type_name not in defined_types:
        log.debug("sync_type creating new type %s", type_name_qualified)
        cql = get_create_type(type_model, ks_name)
        execute(cql)
        cluster.refresh_user_type_metadata(ks_name, type_name)
        type_model.register_for_keyspace(ks_name)
    else:
        defined_fields = defined_types[type_name].field_names
        model_fields = set()
        for field in type_model._fields.values():
            model_fields.add(field.db_field_name)
            if field.db_field_name not in defined_fields:
                execute("ALTER TYPE {0} ADD {1}".format(type_name_qualified, field.get_column_def()))

        type_model.register_for_keyspace(ks_name)

        if len(defined_fields) == len(model_fields):
            log.info("Type %s did not require synchronization", type_name_qualified)
            return

        db_fields_not_in_model = model_fields.symmetric_difference(defined_fields)
        if db_fields_not_in_model:
            log.info("Type %s has fields not referenced by model: %s", type_name_qualified, db_fields_not_in_model)


def get_create_type(type_model, keyspace):
    type_meta = metadata.UserType(keyspace,
                                  type_model.type_name(),
                                  (f.db_field_name for f in type_model._fields.values()),
                                  type_model._fields.values())
    return type_meta.as_cql_query()


def _get_create_table(model):
    ks_table_name = model.column_family_name()
    query_strings = ['CREATE TABLE {0}'.format(ks_table_name)]

    # add column types
    pkeys = []  # primary keys
    ckeys = []  # clustering keys
    qtypes = []  # field types

    def add_column(col):
        s = col.get_column_def()
        if col.primary_key:
            keys = (pkeys if col.partition_key else ckeys)
            keys.append('"{0}"'.format(col.db_field_name))
        qtypes.append(s)

    for name, col in model._columns.items():
        add_column(col)

    qtypes.append('PRIMARY KEY (({0}){1})'.format(', '.join(pkeys), ckeys and ', ' + ', '.join(ckeys) or ''))

    query_strings += ['({0})'.format(', '.join(qtypes))]

    property_strings = []

    _order = ['"{0}" {1}'.format(c.db_field_name, c.clustering_order or 'ASC') for c in model._clustering_keys.values()]
    if _order:
        property_strings.append('CLUSTERING ORDER BY ({0})'.format(', '.join(_order)))

    # options strings use the V3 format, which matches CQL more closely and does not require mapping
    property_strings += metadata.TableMetadataV3._make_option_strings(model.__options__ or {})

    if property_strings:
        query_strings += ['WITH {0}'.format(' AND '.join(property_strings))]

    return ' '.join(query_strings)


def _get_non_pk_field_names(table_meta):
    # returns all fields that aren't part of the PK
    pk_names = set(col.name for col in table_meta.primary_key)
    field_names = [name for name in table_meta.columns.keys() if name not in pk_names]
    return field_names


def _get_table_metadata(model):
    # returns the table as provided by the native driver for a given model
    cluster = get_cluster()
    ks = model._get_keyspace()
    table = model._raw_column_family_name()
    table = cluster.metadata.keyspaces[ks].tables[table]
    return table


def _options_map_from_strings(option_strings):
    # converts options strings to a mapping to strings or dict
    options = {}
    for option in option_strings:
        name, value = option.split('=')
        i = value.find('{')
        if i >= 0:
            value = value[i:value.rfind('}') + 1].replace("'", '"')  # from cql single quotes to json double; not aware of any values that would be escaped right now
            value = json.loads(value)
        else:
            value = value.strip()
        options[name.strip()] = value
    return options


def _update_options(model):
    """Updates the table options for the given model if necessary.

    :param model: The model to update.

    :return: `True`, if the options were modified in Cassandra,
        `False` otherwise.
    :rtype: bool
    """
    log.debug("Checking %s for option differences", model)
    model_options = model.__options__ or {}

    table_meta = _get_table_metadata(model)
    # go to CQL string first to normalize meta from different versions
    existing_option_strings = set(table_meta._make_option_strings(table_meta.options))
    existing_options = _options_map_from_strings(existing_option_strings)
    model_option_strings = metadata.TableMetadataV3._make_option_strings(model_options)
    model_options = _options_map_from_strings(model_option_strings)

    update_options = {}
    for name, value in model_options.items():
        try:
            existing_value = existing_options[name]
        except KeyError:
            raise KeyError("Invalid table option: '%s'; known options: %s" % (name, existing_options.keys()))
        if isinstance(existing_value, six.string_types):
            if value != existing_value:
                update_options[name] = value
        else:
            try:
                for k, v in value.items():
                    if existing_value[k] != v:
                        update_options[name] = value
                        break
            except KeyError:
                update_options[name] = value

    if update_options:
        options = ' AND '.join(metadata.TableMetadataV3._make_option_strings(update_options))
        query = "ALTER TABLE {0} WITH {1}".format(model.column_family_name(), options)
        execute(query)
        return True

    return False


def drop_table(model):
    """
    Drops the table indicated by the model, if it exists.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*
    """
    if not _allow_schema_modification():
        return

    # don't try to delete non existant tables
    meta = get_cluster().metadata

    ks_name = model._get_keyspace()
    raw_cf_name = model._raw_column_family_name()

    try:
        meta.keyspaces[ks_name].tables[raw_cf_name]
        execute('DROP TABLE {0};'.format(model.column_family_name()))
    except KeyError:
        pass


def _allow_schema_modification():
    if not os.getenv(CQLENG_ALLOW_SCHEMA_MANAGEMENT):
        msg = CQLENG_ALLOW_SCHEMA_MANAGEMENT + " environment variable is not set. Future versions of this package will require this variable to enable management functions."
        warnings.warn(msg)
        log.warning(msg)

    return True
