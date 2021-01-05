#/********************************************************************************
# * Copyright (c) 2014, 2018, 2020 Cirrus Link Solutions and others
# *
# * This program and the accompanying materials are made available under the
# * terms of the Eclipse Public License 2.0 which is available at
# * http://www.eclipse.org/legal/epl-2.0.
# *
# * SPDX-License-Identifier: EPL-2.0
# *
# * Contributors:
# *   Cirrus Link Solutions - initial implementation
# *   Justin Brzozoski @ SignalFire Telemetry - major rewrite
# ********************************************************************************/
from tahu import sparkplug_b_pb2
import time
import enum

# TODO - Get rid of enum to allow python2 support?
class Datatype(enum.IntEnum):
	Unknown = 0
	Int8 = 1
	Int16 = 2
	Int32 = 3
	Int64 = 4
	UInt8 = 5
	UInt16 = 6
	UInt32 = 7
	UInt64 = 8
	Float = 9
	Double = 10
	Boolean = 11
	String = 12
	DateTime = 13
	Text = 14
	UUID = 15
	DataSet = 16
	Bytes = 17
	File = 18
	Template = 19

def get_value_field_for_datatype(sparkplug_datatype):
	if sparkplug_datatype in [Datatype.Int8, Datatype.Int16, Datatype.Int32,
							  Datatype.UInt8, Datatype.UInt16]:
		return 'int_value'
	if sparkplug_datatype in [Datatype.Int64,
							  Datatype.UInt32,
							  Datatype.UInt64,
							  Datatype.DateTime]:
		return 'long_value'
	if sparkplug_datatype == Datatype.Float:
		return 'float_value'
	if sparkplug_datatype == Datatype.Double:
		return 'double_value'
	if sparkplug_datatype == Datatype.Boolean:
		return 'boolean_value'
	if sparkplug_datatype in [Datatype.String,
							  Datatype.Text,
							  Datatype.UUID]:
		return 'string_value'
	if sparkplug_datatype in [Datatype.Bytes,
							  Datatype.File]:
		return 'bytes_value'
	if sparkplug_datatype == Datatype.DataSet:
		return 'dataset_value'
	if sparkplug_datatype == Datatype.Template:
		return 'template_value'
	raise ValueError('No value field for datatype {}'.format(sparkplug_datatype))

def get_sparkplug_time(utc_seconds=None):
	if utc_seconds is None:
		utc_seconds = time.clock_gettime(time.CLOCK_REALTIME)
	return int(utc_seconds * 1000)

def sparkplug_to_utc_seconds(sparkplug_time):
	return (float(sparkplug_time) / 1000.0)

def value_to_sparkplug(container,datatype,value):
	# The Sparkplug B protobuf schema doesn't make full use of signed ints or sized ints.
	# We have to do byte-casting because of this when handling many int types.
	# This was tested to work well with Ignition on corner cases.
	# TODO - Should we truncate any outgoing values larger than the datatype supports?
	if datatype == Datatype.Int8:
		bytes = int(value).to_bytes(4,'big',signed=True)
		container.int_value = int().from_bytes(bytes,'big',signed=False)
	elif datatype == Datatype.Int16:
		bytes = int(value).to_bytes(4,'big',signed=True)
		container.int_value = int().from_bytes(bytes,'big',signed=False)
	elif datatype == Datatype.Int32:
		bytes = int(value).to_bytes(4,'big',signed=True)
		container.int_value = int().from_bytes(bytes,'big',signed=False)
	elif datatype == Datatype.Int64:
		bytes = int(value).to_bytes(8,'big',signed=True)
		container.long_value = int().from_bytes(bytes,'big',signed=False)
	elif datatype == Datatype.UInt8:
		container.int_value = value
	elif datatype == Datatype.UInt16:
		container.int_value = value
	elif datatype == Datatype.UInt32:
		container.long_value = value
	elif datatype == Datatype.UInt64:
		container.long_value = value
	elif datatype == Datatype.Float:
		container.float_value = value
	elif datatype == Datatype.Double:
		container.double_value = value
	elif datatype == Datatype.Boolean:
		container.boolean_value = value
	elif datatype == Datatype.String:
		container.string_value = value
	elif datatype == Datatype.DateTime:
		container.long_value = value
	elif datatype == Datatype.Text:
		container.string_value = value
	elif datatype == Datatype.UUID:
		container.string_value = value
	elif datatype == Datatype.Bytes:
		container.bytes_value = value
	elif datatype == Datatype.File:
		container.bytes_value = value
	elif datatype == Datatype.Template:
		container.template_value = value
	else:
		raise ValueError('Unhandled datatype={} in value_to_sparkplug'.format(datatype))

def value_from_sparkplug(container,datatype):
	# The Sparkplug B protobuf schema doesn't make full use of signed ints or sized ints.
	# We have to do byte-casting because of this when handling many int types.
	# This was tested to work well with Ignition on corner cases.
	# We truncate any incoming values larger than the datatype supports.
	if datatype == Datatype.Int8:
		bytes = container.int_value.to_bytes(4,'big',signed=False)
		return int().from_bytes(bytes[-1:],'big',signed=True)
	elif datatype == Datatype.Int16:
		bytes = container.int_value.to_bytes(4,'big',signed=False)
		return int().from_bytes(bytes[-2:],'big',signed=True)
	elif datatype == Datatype.Int32:
		bytes = container.int_value.to_bytes(4,'big',signed=False)
		return int().from_bytes(bytes[-4:],'big',signed=True)
	elif datatype == Datatype.Int64:
		bytes = container.long_value.to_bytes(8,'big',signed=False)
		return int().from_bytes(bytes[-8:],'big',signed=True)
	elif datatype == Datatype.UInt8:
		bytes = container.int_value.to_bytes(4,'big',signed=False)
		return int().from_bytes(bytes[-1:],'big',signed=False)
	elif datatype == Datatype.UInt16:
		bytes = container.int_value.to_bytes(4,'big',signed=False)
		return int().from_bytes(bytes[-2:],'big',signed=False)
	elif datatype == Datatype.UInt32:
		bytes = container.long_value.to_bytes(8,'big',signed=False)
		return int().from_bytes(bytes[-4:],'big',signed=False)
	elif datatype == Datatype.UInt64:
		return container.long_value
	elif datatype == Datatype.Float:
		return container.float_value
	elif datatype == Datatype.Double:
		return container.double_value
	elif datatype == Datatype.Boolean:
		return container.boolean_value
	elif datatype == Datatype.String:
		return container.string_value
	elif datatype == Datatype.DateTime:
		return container.long_value
	elif datatype == Datatype.Text:
		return container.string_value
	elif datatype == Datatype.UUID:
		return container.string_value
	elif datatype == Datatype.Bytes:
		return container.bytes_value
	elif datatype == Datatype.File:
		return container.bytes_value
	elif datatype == Datatype.Template:
		return container.template_value
	raise ValueError('Unhandled datatype={} in value_from_sparkplug'.format(datatype))

def mqtt_params(server,port=1883,
				username=None,password=None,
				client_id=None,keepalive=60,
				tls_enabled=False,ca_certs=None,certfile=None,keyfile=None):
	mqtt_params = {}
	mqtt_params['client_id'] = client_id
	mqtt_params['server'] = server
	mqtt_params['port'] = port
	mqtt_params['username'] = username
	mqtt_params['password'] = password
	mqtt_params['keepalive'] = keepalive
	mqtt_params['tls_enabled'] = tls_enabled
	mqtt_params['ca_certs'] = ca_certs
	mqtt_params['certfile'] = certfile
	mqtt_params['keyfile'] = keyfile
	return mqtt_params

# TODO - Make these dataset and template builder calls useful?

######################################################################
# Helper method for adding dataset metrics to a payload
######################################################################
def initDatasetMetric(payload, name, alias, columns, types):
    metric = payload.metrics.add()
    if name is not None:
        metric.name = name
    if alias is not None:
        metric.alias = alias
    metric.timestamp = int(round(time.time() * 1000))
    metric.datatype = MetricDataType.DataSet

    # Set up the dataset
    metric.dataset_value.num_of_columns = len(types)
    metric.dataset_value.columns.extend(columns)
    metric.dataset_value.types.extend(types)
    return metric.dataset_value
######################################################################

######################################################################
# Helper method for adding dataset metrics to a payload
######################################################################
def initTemplateMetric(payload, name, alias, templateRef):
    metric = payload.metrics.add()
    if name is not None:
        metric.name = name
    if alias is not None:
        metric.alias = alias
    metric.timestamp = int(round(time.time() * 1000))
    metric.datatype = MetricDataType.Template

    # Set up the template
    if templateRef is not None:
        metric.template_value.template_ref = templateRef
        metric.template_value.is_definition = False
    else:
        metric.template_value.is_definition = True

    return metric.template_value
######################################################################

