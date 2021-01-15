#!/usr/bin/python
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
import logging
logging.basicConfig()
logger = logging.getLogger('node_example')
logger.setLevel(logging.DEBUG)
logger.info('Starting Python Sparkplug node demonstration')

import time
from datetime import datetime, timezone
import random
import string
############ BEGIN WORKAROUND
# If the library is installed as a module or you are using PYTHONPATH environment
# variable, these lines modifying the sys.path can be removed.
import sys
sys.path.insert(0, "../../../client_libraries/python/")
############ END WORKAROUND
from tahu import sparkplug_b, node, ignition

### Commonly configured items
myGroupId = "Sparkplug B Devices"
myNodeName = "Python Edge Node 1"
myDeviceName = "Emulated Device"
# You can define multiple connection setups here, and the node will rotate through them
# in response to "Next Server" commands.
myMqttParams = [
	#sparkplug_b.mqtt_params('localhost',username='admin',password='changeme'),
	sparkplug_b.mqtt_params('test.mosquitto.org'),
	sparkplug_b.mqtt_params('broker.hivemq.com'),
]

def sample_cmd_handler(tag, context, value):
	logger.info('sample_cmd_handler tag={} context={} value={}'.format(tag.name, context, value))
	# Do whatever work we need to do...
	# Optionally, echo the value back to the server if you want it to see the change acknowledged
	tag.change_value(value)

def fancier_date_handler(tag, context, value):
	# A simple example of how to get from a received Sparkplug timestamp back to a Python datetime
	dt = datetime.fromtimestamp(sparkplug_b.sparkplug_to_utc_seconds(value), timezone.utc)
	logger.info('fancier_date_handler received {}'.format(str(dt)))
	# We report back the time NOW as the new value, and not the old value or the one we received.
	tag.change_value(sparkplug_b.get_sparkplug_time())

# There is a discrepancy between Ignition as of version 8.1.1 and the Sparkplug spec as of version 2.2.
# The spec says in section 15.2.1 that UInt32 should be stored in the int_value field of the protobuf,
# but Ignition and the reference code have always stored UInt32 in the long_value field.
#
# Our library is flexible and will accept incoming values from either value field gracefully.
# However, outgoing UInt32 can only be done in one or the other.
#
# The u32_in_long parameter to sparkplug_node controls this behavior for a node and everything under it.
# Setting it to True will work in Ignition's style, setting it to False will match the spec's style.
myNode = node.sparkplug_node(myMqttParams,myGroupId,myNodeName,logger=logger,u32_in_long=True)
mySubdevice = node.sparkplug_device(myNode,myDeviceName)

# Here's a quick example of how to define one of each of the basic types:
# The value you pass in now just sets the initial value.
# Hold onto the return object to be able to adjust the reported value later when online.
s8_test_tag = node.sparkplug_metric(mySubdevice, 'int8_test', sparkplug_b.Datatype.Int8, value=-1, cmd_handler=sample_cmd_handler)
s16_test_tag = node.sparkplug_metric(mySubdevice, 'int16_test', sparkplug_b.Datatype.Int16, value=-1, cmd_handler=sample_cmd_handler)
s32_test_tag = node.sparkplug_metric(mySubdevice, 'int32_test', sparkplug_b.Datatype.Int32, value=-1, cmd_handler=sample_cmd_handler)
s64_test_tag = node.sparkplug_metric(mySubdevice, 'int64_test', sparkplug_b.Datatype.Int64, value=-1, cmd_handler=sample_cmd_handler)
u8_test_tag = node.sparkplug_metric(mySubdevice, 'uint8_test', sparkplug_b.Datatype.UInt8, value=1, cmd_handler=sample_cmd_handler)
u16_test_tag = node.sparkplug_metric(mySubdevice, 'uint16_test', sparkplug_b.Datatype.UInt16, value=1, cmd_handler=sample_cmd_handler)
u32_test_tag = node.sparkplug_metric(mySubdevice, 'uint32_test', sparkplug_b.Datatype.UInt32, value=1, cmd_handler=sample_cmd_handler)
u64_test_tag = node.sparkplug_metric(mySubdevice, 'uint64_test', sparkplug_b.Datatype.UInt64, value=1, cmd_handler=sample_cmd_handler)
float_test_tag = node.sparkplug_metric(mySubdevice, 'float_test', sparkplug_b.Datatype.Float, value=1.01, cmd_handler=sample_cmd_handler)
double_test_tag = node.sparkplug_metric(mySubdevice, 'double_test', sparkplug_b.Datatype.Double, value=1.02, cmd_handler=sample_cmd_handler)
boolean_test_tag = node.sparkplug_metric(mySubdevice, 'boolean_test', sparkplug_b.Datatype.Boolean, value=True, cmd_handler=sample_cmd_handler)
string_test_tag = node.sparkplug_metric(mySubdevice, 'string_test', sparkplug_b.Datatype.String, value="Hello, world!", cmd_handler=sample_cmd_handler)
# If you just want the current time you can use sparkplug_b.get_sparkplug_time() without parameters
start_time = sparkplug_b.get_sparkplug_time()
datetime_test_tag = node.sparkplug_metric(mySubdevice, 'datetime_test', sparkplug_b.Datatype.DateTime, value=start_time, cmd_handler=fancier_date_handler)
# If you want to convert from datetime, here's an example:
sample_datetime = datetime(2006, 11, 21, 16, 30, tzinfo=timezone.utc)
alternative_time_sample = sparkplug_b.get_sparkplug_time(sample_datetime.timestamp())

# Properties can be attached to a metric after creating it
property_test_tag = node.sparkplug_metric(mySubdevice, 'property_test', sparkplug_b.Datatype.UInt64, value=23, cmd_handler=sample_cmd_handler)
# You can define them manually, one at a time:
node.metric_property(property_test_tag,'prop_name',sparkplug_b.Datatype.UInt64,value=5,report_with_data=False)
# If you don't need as much control over datatypes, you can define a group all at once via a dictionary
node.bulk_properties(property_test_tag, {'dictstr':'whatever','dictdouble':3.14159,'dictint64':64738})
# And there are helper functions for adding well-known properties that Ignition looks for:
node.ignition_documentation_property(property_test_tag,'A tag for demonstrating lots of property samples!')
node.ignition_low_property(property_test_tag,0)
node.ignition_high_property(property_test_tag,10)
node.ignition_unit_property(property_test_tag,'smoots')
# If you have a property that you need to adjust later on the fly, hold onto the return object when you create it:
property_test_tag_quality = node.ignition_quality_property(property_test_tag)

# Here's an example of making a dataset tag...
# Locally, they are handled as Dataset objects. When making a new one, you pass in a dict matching column names to datatypes
sample_dataset = sparkplug_b.Dataset({'U64Col':sparkplug_b.Datatype.UInt64, 'StrCol':sparkplug_b.Datatype.String, 'DoubleCol':sparkplug_b.Datatype.Double})
# You can manipulate the data with add_rows, get_rows, remove_rows and other methods
sample_dataset.add_rows([[15,'Fifteen',3.14159],[0,'Zero',6.07E27],[65535,'FunFunFun',(2/3)]])
# And the dataset object is pushed onto the metric as the value like normal
dataset_test_tag = node.sparkplug_metric(mySubdevice, 'dataset_sample', sparkplug_b.Datatype.DataSet, value=sample_dataset, cmd_handler=sample_cmd_handler)

myNode.online()
while not myNode.is_connected():
	# TODO - Add some sort of timeout feature?
	pass
loop_count = 0
while True:
	# Sit and wait for a moment...
	time.sleep(5)

	# Send some random data on the string_test tag right away... (triggers an immediate data message)
	new_string = ''.join(random.sample(string.ascii_lowercase,12))
	string_test_tag.change_value(new_string)

	# Next, pile up a few changes all on the same subdevice, and trigger a collected
	# data message containing all of those manually.  (Will not work for tags on different subdevices)

	# Randomly change the quality on the property_test_tag...
	new_quality = random.choice([ignition.QualityCode.Good, ignition.QualityCode.Bad_Stale])
	property_test_tag_quality.change_value(new_quality,send_immediate=False)

	# Report how many times we've gone around this loop in the uint8
	u8_test_tag.change_value(loop_count,send_immediate=False)

	# Send any unsent changes
	mySubdevice.send_data(changed_only=True)

	loop_count = loop_count + 1

