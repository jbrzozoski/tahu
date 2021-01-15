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
# *   Justin Brzozoski @ SignalFire Wireless Telemetry - major rewrite
# ********************************************************************************/
from tahu import sparkplug_b_pb2
import time
import enum

class SparkplugDecodeError(ValueError):
	pass

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
	PropertySet = 20
	PropertySetList = 21

PYTHON_TYPE_PER_DATATYPE = {
	# TODO - Figure out the best way to handle the complex types in this list.
	# For now, they are commented out to help indicate they are non-standard.
	Datatype.Unknown : None,
	Datatype.Int8 : int,
	Datatype.Int16 : int,
	Datatype.Int32 : int,
	Datatype.Int64 : int,
	Datatype.UInt8 : int,
	Datatype.UInt16 : int,
	Datatype.UInt32 : int,
	Datatype.UInt64 : int,
	Datatype.Float : float,
	Datatype.Double : float,
	Datatype.Boolean : bool,
	Datatype.String : str,
	Datatype.DateTime : int,
	Datatype.Text : str,
	Datatype.UUID : str,
	#Datatype.DataSet : lambda x : x,
	Datatype.Bytes : bytes,
	Datatype.File : bytes,
	#Datatype.Template : lambda x : x,
	#Datatype.PropertySet : lambda x : x,
	#Datatype.PropertySetList : lambda x : x,
}

# This is an imperfect list, but useful when you just want to send a
# variable from Python to Sparkplug without thinking too hard about it.
DATATYPE_PER_PYTHONTYPE = {
	int : Datatype.Int64,
	float : Datatype.Double,
	bool : Datatype.Boolean,
	str : Datatype.String,
	bytes : Datatype.Bytes,
}

# NOTE: This is against spec, but is useful when talking to an imperfect
# implementation on the other side.  It lists which value fields we will
# try and read without complaint when we receive a payload.
# TODO - Allow conversion of more types like float->int or int->float?
CONVERTIBLE_VALUE_FIELD_PER_DATATYPE = {
	Datatype.Unknown : (),
	Datatype.Int8 : ('int_value', 'long_value', 'boolean_value'),
	Datatype.Int16 : ('int_value', 'long_value', 'boolean_value'),
	Datatype.Int32 : ('int_value', 'long_value', 'boolean_value'),
	Datatype.Int64 : ('int_value', 'long_value', 'boolean_value'),
	Datatype.UInt8 : ('int_value', 'long_value', 'boolean_value'),
	Datatype.UInt16 : ('int_value', 'long_value', 'boolean_value'),
	Datatype.UInt32 : ('int_value', 'long_value', 'boolean_value'),
	Datatype.UInt64 : ('int_value', 'long_value', 'boolean_value'),
	Datatype.Float : ('float_value', 'double_value'),
	Datatype.Double : ('float_value', 'double_value'),
	Datatype.Boolean : ('int_value', 'long_value', 'boolean_value'),
	Datatype.String : ('string_value'),
	Datatype.DateTime : ('long_value'),
	Datatype.Text : ('string_value'),
	Datatype.UUID : ('string_value'),
	Datatype.DataSet : ('dataset_value'),
	Datatype.Bytes : ('bytes_value'),
	Datatype.File : ('bytes_value'),
	Datatype.Template : ('template_value'),
}

# I could not find these constant limits in Python ...
# It's not in ctypes or anywhere else AFAIK!
MIN_MAX_LIMITS_PER_INTEGER_DATATYPE = {
	Datatype.Int8: (-128, 127),
	Datatype.UInt8: (0, 255),
	Datatype.Int16: (-32768, 32767),
	Datatype.UInt16: (0, 65535),
	Datatype.Int32: (-2147483648, 2147483647),
	Datatype.UInt32: (0, 4294967295),
	Datatype.Int64: (-9223372036854775808, 9223372036854775807),
	Datatype.UInt64: (0, 18446744073709551615),
}

def get_sparkplug_time(utc_seconds=None):
	if utc_seconds is None:
		utc_seconds = time.clock_gettime(time.CLOCK_REALTIME)
	return int(utc_seconds * 1000)

def sparkplug_to_utc_seconds(sparkplug_time):
	return (float(sparkplug_time) / 1000.0)

def value_to_sparkplug(container,datatype,value,u32_in_long=False):
	# The Sparkplug B protobuf schema doesn't make use of signed ints.
	# We have to do byte-casting because of this when handling anything signed.
	# Tests well against Ignition 8.1.1 with u32_in_long=True
	# TODO - Add is_null support if value is None
	# TODO - Should we clamp any outgoing values larger than the datatype supports?
	if u32_in_long and datatype == Datatype.UInt32:
		container.long_value = value
	elif datatype in [Datatype.Int8, Datatype.Int16, Datatype.Int32]:
		bytes = int(value).to_bytes(4,'big',signed=True)
		container.int_value = int().from_bytes(bytes,'big',signed=False)
	elif datatype == Datatype.Int64:
		bytes = int(value).to_bytes(8,'big',signed=True)
		container.long_value = int().from_bytes(bytes,'big',signed=False)
	elif datatype in [Datatype.UInt8, Datatype.UInt16, Datatype.UInt32]:
		container.int_value = value
	elif datatype in [Datatype.UInt64, Datatype.DateTime]:
		container.long_value = value
	elif datatype == Datatype.Float:
		container.float_value = value
	elif datatype == Datatype.Double:
		container.double_value = value
	elif datatype == Datatype.Boolean:
		container.boolean_value = value
	elif datatype in [Datatype.String, Datatype.Text, Datatype.UUID]:
		container.string_value = value
	elif datatype in [Datatype.Bytes, Datatype.File]:
		container.bytes_value = value
	elif datatype == Datatype.Template:
		value.to_sparkplug_template(container.template_value,u32_in_long)
	elif datatype == Datatype.DataSet:
		value.to_sparkplug_dataset(container.dataset_value,u32_in_long)
	else:
		raise ValueError('Unhandled datatype={} in value_to_sparkplug'.format(datatype))

def value_from_sparkplug(container,datatype):
	# The Sparkplug B protobuf schema doesn't make use of signed ints.
	# We have to do byte-casting because of this when handling anything signed.
	# We try to be flexible when handling incoming values because there are some bad
	# implementations out there that might use the wrong value field.
	# We clamp values on any incoming integers larger than the datatype supports.
	# Tests well against Ignition 8.1.1
	try:
		has_null = container.HasField('is_null')
	except ValueError:
		has_null = False
	if has_null and container.is_null:
		return None
	value_field = container.WhichOneof('value')
	if value_field is None:
		raise SparkplugDecodeError('No value field present')
	if value_field not in CONVERTIBLE_VALUE_FIELD_PER_DATATYPE[datatype]:
		raise SparkplugDecodeError('Unexpected value field {} for datatype {}'.format(value_field,datatype))
	value = getattr(container, value_field)
	if datatype in MIN_MAX_LIMITS_PER_INTEGER_DATATYPE:
		value_min, value_max = MIN_MAX_LIMITS_PER_INTEGER_DATATYPE[datatype]
		if value_min < 0:
			# If we're expecting a signed value, we need to cast if reading from int_value or long_value
			# since they are unsigned in the protobuf
			if value_field == 'int_value':
				bytes = value.to_bytes(4,'big',signed=False)
				value = int().from_bytes(bytes,'big',signed=True)
			elif value_field == 'long_value':
				bytes = value.to_bytes(8,'big',signed=False)
				value = int().from_bytes(bytes,'big',signed=True)
		# Now we clamp them to the limits
		if value < value_min:
			value = value_min
		elif value > value_max:
			value = value_max
	if datatype == Datatype.Template:
		return Template.from_sparkplug_template(value)
	if datatype == Datatype.DataSet:
		return Dataset.from_sparkplug_dataset(value)
	if datatype in PYTHON_TYPE_PER_DATATYPE:
		return PYTHON_TYPE_PER_DATATYPE[datatype](value)
	raise SparkplugDecodeError('Unhandled datatype={} in value_from_sparkplug'.format(datatype))

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

# TODO - Add template object type

class Dataset(object):
	# TODO - Add methods to allow easy value access by indices, e.g. with Dataset D you could just reference D[0][0] or D[0][column_name]
	def __init__(self, name_datatype_tuples):
		self._num_columns = len(name_datatype_tuples)
		if self._num_columns == 0:
			raise ValueError('dataset must have at least one column')
		self._column_names = [str(n) for n in name_datatype_tuples.keys()]
		self._column_datatypes = [Datatype(d) for d in name_datatype_tuples.values()]
		self._data = []

	def add_rows(self, data, keyed=False, in_columns=False, insert_index=None):
		"""
		Until I write better docs, here's some samples of the expected formats of data.

		Let's say you have three columns named 'A', 'B', 'C'.
		You want to push in data rows that would look like this in a tabular layout:
		A B C
		1 2 3
		4 5 6
		7 8 9

		Here's the different ways you could pass that in:
		keyed=False in_columns=False data=[[1,2,3],[4,5,6],[7,8,9]]
		keyed=True  in_columns=False data=[{'A':1, 'B':2, 'C':3},{'A':4, 'B':5, 'C':6},{'A':7, 'B':7, 'C':9}]
		keyed=False in_columns=True  data=[[1,4,7],[2,5,8],[3,6,9]]
		keyed=True  in_columns=True  data={'A':[1,4,7], 'B':[2,5,8], 'C':[3,6,9]}

		This convenience provided since I know you don't always have easy ways to get the data in
		one format or another, and you shouldn't have to waste any more of your time re-writing the same
		conversion functions over and over when I could just do it once for you.
		"""
		if ((data is None) or (len(data) == 0)):
			return
		new_data = []
		col_keys = self._columns_names if keyed else range(self._num_columns)
		col_python_types = [PYTHON_TYPE_PER_DATATYPE[self._column_datatypes[x]] for x in range(self._num_columns)]
		col_helper = tuple(zip(col_keys,col_python_types))
		if not in_columns:
			for row in data:
				new_row = []
				for k,t in col_helper:
					new_row.append(t(row[k]))
				new_data.append(new_row)
		else:
			num_rows = len(data[col_keys[0]])
			for k in col_keys[1:]:
				if len(data[k]) != num_rows:
					raise ValueError('data does not have {} rows in all columns'.format(num_rows))
			for row_index in range(num_rows):
				new_row = []
				for k,t in col_helper:
					new_row.append(t(data[k][row_index]))
				new_data.append(new_row)
		if insert_index:
			# This is a neat Python trick.
			# You can assign a new list to a slice of a list, and it will replace
			# the values within the slice with the values from the new list.
			# But if your slice is reduced to size 0, it will just insert the elements at that index.
			self._data[insert_index:insert_index] = new_data
		else:
			self._data.extend(new_data)

	def get_num_columns(self):
		return self._num_columns

	def get_num_rows(self):
		return len(self._data)

	def remove_rows(self, start_index=0, end_index=None, num_rows=None):
		if not end_index:
			end_index = (start_index + num_rows) if num_rows else len(self._data)
		self._data[start_index:end_index] = []

	def get_rows(self, start_index=0, end_index=None, num_rows=None, in_columns=False, keyed=False):
		"""
		Go see the comments on add_rows to understand the output format relative to in_columns and keyed
		"""
		if not end_index:
			end_index = (start_index + num_rows) if num_rows else len(self._data)
		if not in_columns:
			if keyed:
				return [dict(zip(self._column_names, row)) for row in self._data[start_index:end_index]]
			return self._data[start_index:end_index]
		if not keyed:
			data = []
			for k in range(self._num_columns):
				data.append([self._data[r][k] for r in range(start_index, end_index)])
			return data
		data = {}
		for k in range(len(self._column_names)):
			data[self._column_names[k]] = [self._data[r][k] for r in range(start_index, end_index)]
		return data

	def to_sparkplug_dataset(self,sp_dataset,u32_in_long=False):
		sp_dataset.num_of_columns = self._num_columns
		sp_dataset.columns.extend(self._column_names)
		sp_dataset.types.extend(self._column_datatypes)
		for data_row in self._data:
			sp_row = sp_dataset.rows.add()
			for c in range(self._num_columns):
				dataset_value = sp_row.elements.add()
				value_to_sparkplug(dataset_value,self._column_datatypes[c],data_row[c],u32_in_long)
		return sp_dataset

	@classmethod
	def from_sparkplug_dataset(cls, sp_dataset):
		try:
			new_dataset = cls(dict(zip(sp_dataset.columns, sp_dataset.types)))
		except ValueError as errmsg:
			raise SparkplugDecodeError(errmsg)
		for sp_row in sp_dataset.rows:
			new_row = []
			for c in range(new_dataset._num_columns):
				value = value_from_sparkplug(sp_row.elements[c], new_dataset._column_datatypes[c])
				new_row.append(value)
			new_dataset._data.append(new_row)
		return new_dataset

