# Tested on Python 3.8.5

from enum import Enum
import threading
import logging
import time
import paho.mqtt.client as mqtt
from tahu import sparkplug_b, sparkplug_b_pb2, ignition

def _rebirth_command_handler(tag, context, value):
	tag._logger.info('Rebirth command received')
	assert(isinstance(tag._parent_device,sparkplug_node))
	# We don't care what value the server wrote to the tag, any write is considered a trigger.
	tag._parent_device._needs_to_send_birth = True

def _next_server_command_handler(tag, context, value):
	tag._logger.info('Next Server command received')
	assert(isinstance(tag._parent_device,sparkplug_node))
	# We don't care what value the server wrote to the tag, any write is considered a trigger.
	tag._parent_device._mqtt_param_index = (tag._parent_device._mqtt_param_index + 1) % len(tag._parent_device._mqtt_params)
	tag._parent_device._reconnect_client = True

class sparkplug_metric(object):
	def __init__(self,parent_device,name,datatype=None,value=None,cmd_handler=None,cmd_context=None):
		# TODO - Protect the name/alias from being changed after creation
		# TODO - Add support for custom properties, move existing properties into that system...
		if datatype is None and value is None:
			raise ValueError('Unable to define metric without explicit datatype or initial value')
		self._parent_device  = parent_device
		self._logger         = parent_device._logger
		self._u32_in_long    = parent_device._u32_in_long
		self.name            = str(name)
		if datatype:
			self._datatype = sparkplug_b.Datatype(datatype)
		else:
			if not type(value) in sparkplug_b.DATATYPE_PER_PYTHONTYPE:
				raise ValueError('Need explicit datatype for Python type {}'.format(type(value)))
			self._datatype = sparkplug_b.DATATYPE_PER_PYTHONTYPE[type(value)]

		self._value          = value
		self._last_received  = None
		self._last_sent      = None
		self._cmd_handler    = cmd_handler
		self._cmd_context    = cmd_context
		self._properties     = []
		self.alias           = parent_device._attach_tag(self)

	def _attach_property(self,property):
		next_index = len(self._properties)
		self._properties.append(property)
		# TODO - Add checking/handling depending if we're connected
		return next_index

	def _fill_in_payload_metric(self,new_metric,birth=False):
		if birth:
			new_metric.name = self.name
		new_metric.alias = self.alias
		new_metric.datatype = self._datatype
		# Add properties
		for p in self._properties:
			# This chunk could arguably be a method of the property, but I
			# felt it made more sense here because of the way the
			# PropertySet protobuf object works...
			if birth or p._report_with_data:
				new_metric.properties.keys.append(p._name)
				pvalue = new_metric.properties.values.add()
				pvalue.type = p._datatype
				sparkplug_b.value_to_sparkplug(pvalue,pvalue.type,p._value,self._u32_in_long)
				p._last_sent = p._value
		# Add the current value or set is_null if None
		if self._value is None:
			new_metric.is_null = True
		else:
			sparkplug_b.value_to_sparkplug(new_metric,self._datatype,self._value,self._u32_in_long)
		self._last_sent = self._value

	def change_value(self,value,send_immediate=True):
		self._value = value
		if send_immediate:
			self._parent_device.send_data([self.alias])
		return self.alias

	def _handle_sparkplug_command(self,sparkplug_metric):
		# Note that we enforce OUR expected datatype on the value as we pull it from the metric
		try:
			value = sparkplug_b.value_from_sparkplug(sparkplug_metric,self._datatype)
		except sparkplug_b.SparkplugDecodeError as errmsg:
			self._logger.warning('Sparkplug decode error for tag {}: {}'.format(self.name,errmsg))
			return
		self._logger.debug('Command received for tag {} = {}'.format(self.name, value))
		if self._cmd_handler:
			self._cmd_handler(self, self._cmd_context, value)
		else:
			self._logger.info('Received command for tag {} with no handler. No action taken.'.format(self.name))
		self._last_received = value

	def changed_since_last_sent(self):
		# Check all report_with_data properties
		for p in self._properties:
			if p._report_with_data and p.changed_since_last_sent():
				return True
		return (self._value != self._last_sent)

class metric_property(object):
	def __init__(self, parent_metric, name, datatype, value, report_with_data=False):
		self._parent_metric = parent_metric
		self._name = str(name)
		if datatype:
			self._datatype = sparkplug_b.Datatype(datatype)
		else:
			if not type(value) in sparkplug_b.DATATYPE_PER_PYTHONTYPE:
				raise ValueError('Need explicit datatype for Python type {}'.format(type(value)))
			self._datatype = sparkplug_b.DATATYPE_PER_PYTHONTYPE[type(value)]
		self._value = value
		self._report_with_data = bool(report_with_data)
		self._last_sent = None
		self._parent_metric._attach_property(self)

	def changed_since_last_sent(self):
		return (self._value != self._last_sent)

	def change_value(self, value, send_immediate=False):
		# TODO - Trigger rebirth if someone changes a property that is not report_with_data?
		self._value = value
		if self._report_with_data and send_immediate:
			self._parent_metric._parent_device.send_data([self._parent_metric.alias])
		return self._parent_metric.alias

def bulk_properties(parent_metric, property_dict):
	return [metric_property(parent_metric,name,None,property_dict[name],False) for name in property_dict.keys()]

def ignition_quality_property(parent_metric, value=ignition.QualityCode.Good):
	return metric_property(parent_metric, 'Quality', sparkplug_b.Datatype.Int32, value, True)

def ignition_low_property(parent_metric, value):
	return metric_property(parent_metric, 'engLow', parent_metric._datatype, value, False)

def ignition_high_property(parent_metric, value):
	return metric_property(parent_metric, 'engHigh', parent_metric._datatype, value, False)

def ignition_unit_property(parent_metric, value):
	return metric_property(parent_metric, 'engUnit', sparkplug_b.Datatype.String, value, False)

def ignition_documentation_property(parent_metric, value):
	return metric_property(parent_metric, 'Documentation', sparkplug_b.Datatype.String, value, False)

class _sparkplug_base_device(object):
	def __init__(self):
		self._mqtt_client = None
		self._tags = []
		self._needs_to_send_birth = True

	def get_tag_names(self):
		return [m.name for m in self._tags]

	def _get_next_seq(self):
		raise NotImplementedError('_get_next_seq not implemented on this class')

	def _attach_tag(self,tag):
		next_index = len(self._tags)
		self._tags.append(tag)
		if self.is_connected():
			self.send_death()
		self._needs_to_send_birth = True
		return next_index

	# TODO - Add another function to remove a tag

	def _get_payload(self,alias_list,birth):
		tx_payload = sparkplug_b_pb2.Payload()
		tx_payload.timestamp = sparkplug_b.get_sparkplug_time()
		tx_payload.seq = self._get_next_seq()
		if birth:
			alias_list = range(len(self._tags))
		for m in alias_list:
			new_metric = tx_payload.metrics.add()
			self._tags[m]._fill_in_payload_metric(new_metric,birth=birth)
		return tx_payload

	def _get_topic(self,cmd_type):
		raise NotImplementedError('_get_topic not implemented on this class')

	def send_birth(self):
		raise NotImplementedError('send_birth not implemented on this class')

	def send_death(self):
		raise NotImplementedError('send_death not implemented on this class')

	def send_data(self, aliases=None, changed_only=False):
		if not self.is_connected():
			self._logger.warning('Trying to send data when not connected. Skipping.')
			return
		if self._needs_to_send_birth:
			return self.send_birth()
		if aliases is None:
			aliases = range(len(self._tags))
		if changed_only:
			aliases = [x for x in aliases if self._tags[x].changed_since_last_sent()]

		tx_payload = self._get_payload(aliases,False)
		topic = self._get_topic('DATA')
		return self._mqtt_client.publish(topic,tx_payload.SerializeToString())

	def get_watched_topic(self):
		return self._get_topic('CMD')

	def _handle_payload(self,topic,payload):
		# Check if topic is for this device
		watched_topic = self.get_watched_topic()
		if topic != watched_topic:
			return False
		local_names = self.get_tag_names()
		for pm in payload.metrics:
			if pm.HasField('alias'):
				if pm.alias >= len(self._tags):
					self._logger.warning('Invalid alias {} for this device. Skipping metric.'.format(pm.alias))
					continue
				self._tags[pm.alias]._handle_sparkplug_command(pm)
			elif pm.HasField('name'):
				if not pm.name in local_names:
					self._logger.warning('Invalid name {} for this device. Skipping metric.'.format(pm.name))
					continue
				self._tags[local_names.index(pm.name)]._handle_sparkplug_command(pm)
			else:
				self._logger.warning('No name or alias provided. Skipping metric.')
				continue
		# Even if the payload was corrupt/weird, the topic was for us.
		# We can return True to let the caller know it was handled
		return True

	def is_connected(self):
		raise NotImplementedError('is_connected not implemented on this class')

class sparkplug_node(_sparkplug_base_device):
	def __init__(self,mqtt_params,group_id,edge_node_id,provide_bdSeq=True,provide_controls=True,logger=None,u32_in_long=False):
		super().__init__()
		self._mqtt_params = list(mqtt_params)
		self._mqtt_param_index = 0
		self._u32_in_long = bool(u32_in_long)
		self._group_id         = str(group_id)
		self._edge_node_id     = str(edge_node_id)
		node_reference = '{}_{}'.format(self._group_id,self._edge_node_id)
		self._logger = logger if logger else logging.getLogger(node_reference)
		self._mqtt_logger = self._logger.getChild('mqtt')
		self._init_mqtt_client()
		self._sequence         = 0
		self._subdevices       = []
		self._all_device_topics = [ self.get_watched_topic() ]
		self._thread = None
		self._thread_terminate = True
		self._reconnect_client = False

		if provide_bdSeq:
			new_tag = sparkplug_metric(self,'bdSeq',sparkplug_b.Datatype.Int64,value=sparkplug_b.get_sparkplug_time())
			self._bdseq_alias = new_tag.alias
		else:
			self._bdseq_alias = None
		if provide_controls:
			#sparkplug_metric(self,'Node Control/Reboot',sparkplug_b.Datatype.Boolean,value=False)
			sparkplug_metric(self,'Node Control/Rebirth',sparkplug_b.Datatype.Boolean,value=False,cmd_handler=_rebirth_command_handler)
			sparkplug_metric(self,'Node Control/Next Server',sparkplug_b.Datatype.Boolean,value=False,cmd_handler=_next_server_command_handler)

	def _get_next_seq(self):
		seq_to_use = self._sequence
		self._sequence = (self._sequence + 1) % 256
		return seq_to_use

	def send_birth(self):
		if not self.is_connected():
			self._logger.warning('Trying to send birth when not connected. Skipping.')
			return
		self._sequence = 0
		tx_payload = self._get_payload(None,True)
		topic = self._get_topic('BIRTH')
		pub_result = self._mqtt_client.publish(topic,tx_payload.SerializeToString())
		if pub_result.rc != 0:
			return pub_result
		self._needs_to_send_birth = False
		for d in self._subdevices:
			d._needs_to_send_birth = True
		return pub_result

	def _get_death_payload(self,will):
		if self._bdseq_alias is not None:
			if will:
				new_bdseq = sparkplug_b.get_sparkplug_time()
				self._logger.debug('Generating new WILL bdSeq={}'.format(new_bdseq))
				self._tags[self._bdseq_alias].change_value(new_bdseq,send_immediate=False)
			death_payload = self._get_payload([self._bdseq_alias],False)
			# This timestamp would be wrong when finally sent, so we just remove it
			death_payload.ClearField('timestamp')
			# To be safe, put the name on bdSeq metric and not just the alias
			# (Ignition seems to need the name...)
			death_payload.metrics[0].name = 'bdSeq'
		else:
			death_payload = self._get_payload([],False)
		return death_payload

	def _get_will_topic_and_payload(self):
		tx_payload = self._get_death_payload(will=True)
		topic = self._get_topic('DEATH')
		return topic, tx_payload.SerializeToString()

	def send_death(self):
		if not self.is_connected():
			self._logger.warning('Trying to send death when not connected. Skipping.')
			return
		tx_payload = self._get_death_payload(will=False)
		topic = self._get_topic('DEATH')
		pub_result = self._mqtt_client.publish(topic,tx_payload.SerializeToString())
		# Even if this publish didn't succeed, it's safer to rebirth unnecessarily...
		self._needs_to_send_birth = True
		for d in self._subdevices:
			d.needs_to_birth = True
		return pub_result

	def _attach_subdevice(self, subdevice):
		next_index = len(self._subdevices)
		self._subdevices.append(subdevice)
		self._all_device_topics.append(subdevice.get_watched_topic())
		if self.is_connected():
			self.send_death()
		self._needs_to_send_birth = True
		return next_index

	# TODO - Add another function to remove a subdevice

	def _get_topic(self, cmd_type):
		return 'spBv1.0/{}/N{}/{}'.format(self._group_id,cmd_type,self._edge_node_id)

	def _mqtt_subscribe(self):
		# TODO - Add support for 'STATE/#' monitoring and holdoff?
		# Subscribe to all topics for commands related to this device...
		ncmd_subscription = 'spBv1.0/{}/NCMD/{}/#'.format(self._group_id,self._edge_node_id)
		dcmd_subscription = 'spBv1.0/{}/DCMD/{}/#'.format(self._group_id,self._edge_node_id)
		desired_qos = 0
		topic = [(ncmd_subscription,desired_qos),(dcmd_subscription,desired_qos)]
		return self._mqtt_client.subscribe(topic)

	def _mqtt_on_connect(self,client,userdata,flags,rc):
		if rc != 0:
			self._logger.warning('MQTT connect error rc={}'.format(rc))
			return
		self._is_connected = True
		# A fresh connection implies we have no subscriptions and need to birth
		self._is_subscribed = False
		self._needs_to_send_birth = True
		for d in self._subdevices:
			d._needs_to_send_birth = True
		self._mqtt_subscribe()

	def _mqtt_on_disconnect(self, client, userdata, rc):
		self._logger.warning('MQTT disconnect rc={}'.format(rc))
		self._is_connected = False
		# The thread loop will try reconnecting for us, we just need to setup a new will first
		will_topic, will_payload = self._get_will_topic_and_payload()
		client.will_set(will_topic, will_payload)

	def _mqtt_on_message(self, client, userdata, message):
		if message.topic in self._all_device_topics:
			rx_payload = sparkplug_b_pb2.Payload()
			rx_payload.ParseFromString(message.payload)
			handler_index = self._all_device_topics.index(message.topic)
			if handler_index == 0:
				self._handle_payload(message.topic,rx_payload)
			else:
				self._subdevices[(handler_index-1)]._handle_payload(message.topic,rx_payload)
		else:
			self._logger.info('Ignoring MQTT message on topic {}'.format(message.topic))

	def _mqtt_on_subscribe(self, client, userdata, mid, granted_qos):
		# TODO - Confirm the mid matches our subscription request before assuming we're good to go?
		self._is_subscribed = True

	def _init_mqtt_client(self,reinit=False):
		import os
		curr_params = self._mqtt_params[self._mqtt_param_index]
		if curr_params['client_id']:
			self._client_id = curr_params['client_id']
		else:
			self._client_id = '{}_{}_{}'.format(self._group_id, self._edge_node_id, os.getpid())
		self._logger.info('Initializing MQTT client (client_id={} reinit={})'.format(self._client_id, reinit))
		if reinit:
			self._mqtt_client.reinitialise(client_id=self._client_id)
		else:
			self._mqtt_client = mqtt.Client(client_id=self._client_id)
		self._mqtt_client.enable_logger(self._mqtt_logger)
		self._mqtt_client.on_connect = self._mqtt_on_connect
		self._mqtt_client.on_disconnect = self._mqtt_on_disconnect
		self._mqtt_client.on_message = self._mqtt_on_message
		self._mqtt_client.on_subscribe = self._mqtt_on_subscribe
		self._is_connected = False
		self._is_subscribed = False

	def _prep_client_connection(self):
		if self._is_connected:
			self._logger.error('Attempting to start MQTT connection while already connected. Skipping.')
			return
		curr_params = self._mqtt_params[self._mqtt_param_index]
		if curr_params['username']:
			self._mqtt_client.username_pw_set(curr_params['username'], curr_params['password'])
		if (curr_params['port'] == 1883 and curr_params['tls_enabled']) or (curr_params['port'] == 8883 and not curr_params['tls_enabled']):
			self._logger.warning('Setting up MQTT params on well-known port with unexpected TLS setting. Are you sure you meant to do this?')
		if curr_params['tls_enabled']:
			self._mqtt_client.tls_set(ca_certs=curr_params['ca_certs'],
									 certfile=curr_params['certfile'],
									 keyfile=curr_params['keyfile'])
		will_topic, will_payload = self._get_will_topic_and_payload()
		self._mqtt_client.will_set(will_topic, will_payload)
		self._logger.info('Starting MQTT client connection to host={}'.format(curr_params['server']))
		self._mqtt_client.connect(host=curr_params['server'],
								  port=curr_params['port'],
								  keepalive=curr_params['keepalive'])

	def _thread_main(self):
		# TODO - Add support to timeout bad/failed connections to trigger _reconnect_client
		self._logger.info('MQTT thread started...')
		self._prep_client_connection()
		while not self._thread_terminate:
			self._mqtt_client.loop()
			if self._reconnect_client:
				self._reconnect_client = False
				self._init_mqtt_client(reinit=True)
				self._prep_client_connection()
			elif self.is_connected():
				if self._needs_to_send_birth:
					self.send_birth()
				else:
					# Only try to send subdevice births if the top-level device doesn't need it
					for d in self._subdevices:
						if d._needs_to_send_birth:
							d.send_birth()
		# Use the reinit as a trick to force the sockets closed
		self._init_mqtt_client(reinit=True)
		self._logger.info('MQTT thread stopped...')

	def online(self):
		if self._thread is not None:
			self._logger.warning('MQTT thread already running!')
			return
		self._thread_terminate = False
		self._thread = threading.Thread(target=self._thread_main)
		self._thread.daemon = True
		self._thread.start()

	def offline(self):
		self._logger.info('Requesting MQTT thread stop...')
		self._thread_terminate = True
		if self._thread is None:
			self._logger.warning('MQTT thread not running!')
		elif threading.current_thread() != self._thread:
			self._thread.join()
			self._thread = None

	def is_connected(self):
		return self._is_connected and self._is_subscribed

class sparkplug_device(_sparkplug_base_device):
	def __init__(self,parent_device,name):
		super().__init__()
		# TODO - Protect the name from being changed after creation
		self.name           = str(name)
		self._parent_device = parent_device
		self._logger        = parent_device._logger.getChild(self.name)
		self._mqtt_client   = parent_device._mqtt_client
		self._mqtt_logger   = parent_device._mqtt_logger
		self._u32_in_long   = parent_device._u32_in_long
		self._parent_index  = self._parent_device._attach_subdevice(self)

	def _get_next_seq(self):
		return self._parent_device._get_next_seq()

	def send_birth(self):
		if not self.is_connected():
			self._logger.warning('Trying to send birth when not connected. Skipping.')
			return
		# If the parent device also needs to birth, do that first!
		if self._parent_device._needs_to_send_birth:
			return self._parent_device.send_birth()
		tx_payload = self._get_payload(None, True)
		topic = self._get_topic('BIRTH')
		pub_result = self._mqtt_client.publish(topic,tx_payload.SerializeToString())
		if pub_result.rc == 0:
			self._needs_to_send_birth = False
		return pub_result

	def send_death(self):
		if not self.is_connected():
			self._logger.warning('Trying to send death when not connected. Skipping.')
			return
		tx_payload = self._get_payload([],False)
		topic = self._get_topic('DEATH')
		pub_result = self._mqtt_client.publish(topic,tx_payload.SerializeToString())
		# Even if this publish didn't succeed, it's safer to rebirth unnecessarily...
		self._needs_to_send_birth = True
		return pub_result

	def _get_topic(self, cmd_type):
		return 'spBv1.0/{}/D{}/{}/{}'.format(self._parent_device._group_id,cmd_type,self._parent_device._edge_node_id,self.name)

	def is_connected(self):
		return self._parent_device.is_connected()

