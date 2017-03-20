#!/usr/bin/env python

import dbus
import dbus.service
import dbus.mainloop.glib
from dbus.exceptions import DBusException
from gi.repository import GObject as gobject
import threading
import time
from datetime import datetime
import copy

import logging
log = logging.getLogger('scadasim')

class DBusClient(object):

    def __init__(self, hostname=None):
        import socket
        self.hostname = hostname
        if not hostname:
            self.hostname = socket.gethostname()
        self.connect_to_dbus()

    def connect_to_dbus(self):
        while True:
            try:
                log.debug("Connecting to dbus")
                self.bus = dbus.SystemBus()
                self.remote_object = self.bus.get_object("com.root9b.scadasim", "/")
                self.iface = dbus.Interface(self.remote_object, "com.root9b.scadasim")
                self._registerPLC = self.iface.registerPLC
                self._readSensors = self.iface.readSensors
                self._setValues = self.iface.setValues
                return True
            except DBusException:
                log.error("Dbus error. Verify that the scadasim simulator is running and registered on the dbus.")
                log.error("Retrying in 5 seconds...")
                pass
            time.sleep(5)


    def registerPLC(self, plcname=None):
        if not plcname:
            plcname = self.hostname

        while True:
            try:
                return self._registerPLC(plcname)
            except DBusException:
                self.connect_to_dbus()
                pass

    def readSensors(self, plcname=None):
        if not plcname:
            plcname = self.hostname

        while True:
            try:
                return self._readSensors(plcname)
            except DBusException:
                self.connect_to_dbus()
                pass

    def setValues(self, fx, address, values, plcname=None):
        if not plcname:
            plcname = self.hostname

        while True:
            try:
                return self._setValues(plcname, fx, address, values)
            except DBusException:
                self.connect_to_dbus()
                pass

    def introspect(self):
        print self.remote_object.Introspect(dbus_interface="org.freedesktop.DBus.Introspectable")

class DBusService(object):

    def __init__(self):
        super(DBusService, self).__init__()
        self._stop = threading.Event()
        self.sensors = None
        self.plcs = None
        self.read_frequency = 0.5
        self.speed = 1
        self.active = True
        self.read_sensor_thread = None

        log.debug('Starting dbus main thread')
        self.dbus_loop_thread = threading.Thread(target=self.bus_loop)
        self.dbus_loop_thread.daemon = True

    def start(self):
        log.debug('Starting read sensors worker thread')
        self.read_sensor_thread = threading.Thread(target=self._read_sensors)
        self.read_sensor_thread.daemon = True
        self.read_sensor_thread.start()
        if not self.dbus_loop_thread.isAlive():
            self.dbus_loop_thread.start()

    def bus_loop(self):
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
        self.db = DBusWorker(self.plcs)
        self.db.loop.run()

    def set_speed(self, speed):
        self.speed = speed

    def load_plcs(self, plcs):
        self.plcs = plcs

    def _read_sensors(self):
        while not self._stop.is_set():
            log.debug("%s Reading Sensors %s" % (self, datetime.now()))

            for plc in self.plcs:
                for sensor in self.plcs[plc]['sensors']:
                    read_sensor = self.plcs[plc]['sensors'][sensor]['read_sensor']
                    self.plcs[plc]['sensors'][sensor]['value'] = read_sensor()

            # Calculate the next run time based on simulation speed and read frequency
            delay = (-time.time()%(self.speed*self.read_frequency))
            time.sleep(delay)

    def activate(self):
        self._stop.clear()
        self.start()

    def deactivate(self):
        self._stop.set()
        self.read_sensor_thread.join()

 
class DBusWorker(dbus.service.Object):

    def __init__(self, plcs):
        self.session_bus = dbus.SystemBus()
        self.name = dbus.service.BusName("com.root9b.scadasim", bus=self.session_bus)
        self.loop = gobject.MainLoop()
        self.plcs = plcs
        
        dbus.service.Object.__init__(self, self.name, '/')

    """
    'di' - Discrete Inputs initializer 'co' - Coils initializer 'hr' - Holding Register initializer 'ir' - Input Registers iniatializer

    Coil/Register Numbers   Data Addresses  Type        Table Name                          Use
    1-9999                  0000 to 270E    Read-Write  Discrete Output Coils               on/off read/write   co
    10001-19999             0000 to 270E    Read-Only   Discrete Input Contacts             on/off readonly     di
    30001-39999             0000 to 270E    Read-Only   Analog Input Registers              analog readonly     ir
    40001-49999             0000 to 270E    Read-Write  Analog Output Holding Registers     analog read/write   hr

    Each coil or contact is 1 bit and assigned a data address between 0000 and 270E.
    Each register is 1 word = 16 bits = 2 bytes

    dbus-send --system --type=method_call --print-reply --dest=com.root9b.scadasim / org.freedesktop.DBus.Introspectable.Introspect

    """

    #https://dbus.freedesktop.org/doc/dbus-python/doc/tutorial.html#basic-type-conversions
    @dbus.service.method("com.root9b.scadasim", in_signature='s', out_signature='q')
    def registerPLC(self, plc):
        """
            return sensor name and sensor address in PLC.
            TODO: add slave id

        dbus-send --system --print-reply --dest=com.root9b.scadasim / com.root9b.scadasim.registerPLC string:"hello"
        """
        self.plcs[plc]['registered'] = True
        return int(self.plcs[plc]['slaveid'])

    @dbus.service.method("com.root9b.scadasim", in_signature='s', out_signature='a{sa{sv}}')
    def readSensors(self, plc):
        sensors = copy.deepcopy(self.plcs[plc]['sensors'])
        for sensor in sensors:
            # Remove the read_sensor method to avoid parsing errors
            sensors[sensor].pop('read_sensor', None)
            sensors[sensor].pop('write_sensor', None)
        return sensors

    @dbus.service.method("com.root9b.scadasim", in_signature='squaq', out_signature='b')
    def setValues(self, plc, fx, address, values):
        if not hasattr(values,"__iter__"): values = [ values ]

        __fx_mapper = {2: 'd', 4: 'i'}
        __fx_mapper.update([(i, 'h') for i in [3, 6, 16, 22, 23]])
        __fx_mapper.update([(i, 'c') for i in [1, 5, 15]])

        register = __fx_mapper[fx]

        if register == 'c' or register == 'd':
            values = map(bool, values)
        elif register == 'i' or register == 'h':
            values = map(int, values)
        else:
            return False

        retval = False
        for offset in range(len(values)):
            # If multiple values provided, try to write them all
            retval |= self._write_sensor(plc, register, address+offset, values[offset])
        return retval

    def _write_sensor(self, plc, register, address, value):

        for sensor in self.plcs[plc]['sensors']:
            s = self.plcs[plc]['sensors'][sensor]
            if address == s['data_address'] and register == s['register_type']:
                write_sensor = self.plcs[plc]['sensors'][sensor]['write_sensor']
                write_sensor(value)
                return True
        return False


if __name__ == '__main__':
    db = DBusService()
    db.start()
