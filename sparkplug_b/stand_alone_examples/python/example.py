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
# TODO - Add support to timeout bad/failed connections to the next server as well
myMqttParams = [
	#sparkplug_b.mqtt_params('localhost',username='admin',password='changeme'),
	sparkplug_b.mqtt_params('test.mosquitto.org'),
	sparkplug_b.mqtt_params('broker.hivemq.com'),
]

def sample_cmd_handler(tag, context, value):
	# TODO - Add methods to get the name and alias from a tag object
	logger.info('sample_cmd_handler tag={} context={} value={}'.format(tag._name, context, value))
	# Do whatever work we need to do...
	# Optionally, echo the value back to the server if you want it to see the change acknowledged
	tag.change_value(value)

def fancier_date_handler(tag, context, value):
    # A simple example of how to get from a received Sparkplug timestamp back to a Python datetime
	dt = datetime.fromtimestamp(sparkplug_b.sparkplug_to_utc_seconds(value), timezone.utc)
	logger.info('fancier_date_handler received {}'.format(str(dt)))
	# We report back the time NOW as the new value, and not the old value or the one we received.
	tag.change_value(sparkplug_b.get_sparkplug_time())

myNode = node.sparkplug_edge_device(myMqttParams,myGroupId,myNodeName,logger=logger)
mySubdevice = node.sparkplug_subdevice(myNode,myDeviceName)
s8_test_tag = node.sparkplug_tag(mySubdevice, 'int8_test', sparkplug_b.Datatype.Int8, value=-1, cmd_handler=sample_cmd_handler)
s16_test_tag = node.sparkplug_tag(mySubdevice, 'int16_test', sparkplug_b.Datatype.Int16, value=-1, cmd_handler=sample_cmd_handler)
s32_test_tag = node.sparkplug_tag(mySubdevice, 'int32_test', sparkplug_b.Datatype.Int32, value=-1, cmd_handler=sample_cmd_handler)
s64_test_tag = node.sparkplug_tag(mySubdevice, 'int64_test', sparkplug_b.Datatype.Int64, value=-1, cmd_handler=sample_cmd_handler)
u8_test_tag = node.sparkplug_tag(mySubdevice, 'uint8_test', sparkplug_b.Datatype.UInt8, value=1, cmd_handler=sample_cmd_handler)
u16_test_tag = node.sparkplug_tag(mySubdevice, 'uint16_test', sparkplug_b.Datatype.UInt16, value=1, cmd_handler=sample_cmd_handler)
u32_test_tag = node.sparkplug_tag(mySubdevice, 'uint32_test', sparkplug_b.Datatype.UInt32, value=1, cmd_handler=sample_cmd_handler)
u64_test_tag = node.sparkplug_tag(mySubdevice, 'uint64_test', sparkplug_b.Datatype.UInt64, value=1, cmd_handler=sample_cmd_handler)
float_test_tag = node.sparkplug_tag(mySubdevice, 'float_test', sparkplug_b.Datatype.Float, value=1.01, cmd_handler=sample_cmd_handler)
double_test_tag = node.sparkplug_tag(mySubdevice, 'double_test', sparkplug_b.Datatype.Double, value=1.02, cmd_handler=sample_cmd_handler)
boolean_test_tag = node.sparkplug_tag(mySubdevice, 'boolean_test', sparkplug_b.Datatype.Boolean, value=True, cmd_handler=sample_cmd_handler)
string_test_tag = node.sparkplug_tag(mySubdevice, 'string_test', sparkplug_b.Datatype.String, value="Hello, world!", cmd_handler=sample_cmd_handler)
# A simple example of how to get from a Python datetime to a Sparkplug timestamp
first_time_report = datetime(2006, 11, 21, 16, 30, tzinfo=timezone.utc)
first_time_report = sparkplug_b.get_sparkplug_time(first_time_report.timestamp())
datetime_test_tag = node.sparkplug_tag(mySubdevice, 'datetime_test', sparkplug_b.Datatype.DateTime, value=first_time_report, cmd_handler=fancier_date_handler)
myNode.online()
# TODO - Add a method to find out if a device/tag is online and connected
while not myNode._is_connected:
	# TODO - Add some sort of timeout feature?
	pass
loop_count = 0
while True:
	# Sit and wait for a moment...
	time.sleep(5)

	# Send some random data on the string_test tag right away... (triggers an immediate data message)
	string_test_tag.change_value(''.join(random.sample(string.ascii_lowercase,12)))

	# Next, pile up a few changes all on the same subdevice, and trigger a collected
	# data message containing all of those manually.  (Will not work for tags on different subdevices)
	aliases = []

	# Randomly change the quality on the double_test tag...
	aliases.append(double_test_tag.change_quality(random.choice([ignition.QualityCode.Good, ignition.QualityCode.Bad_Stale]),send_immediate=False))

	# Report how many times we've gone around this loop in the uint8
	aliases.append(u8_test_tag.change_value(loop_count,send_immediate=False))

	# Send the collected message
	# TODO - Add a feature for the devices to manage a list of unsent tags internally?
	mySubdevice.send_data(aliases)

	loop_count = loop_count + 1





