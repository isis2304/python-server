# -*- coding: utf-8 -*-

import json
import time
import pika
import logging
import hashlib
import datetime
from pika import adapters
from model.vos import operacion
from model.fachada import bancandes


class ExampleConsumer(object):
    """This == an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel == closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """
    EXCHANGE = 'transactions'
    EXCHANGE_TYPE = 'topic'
    QUEUE = 'llamabank'
    ROUTING_KEY = 'bancandes.requests'

    def __init__(self, logger, publisher, amqp_url='amqp://llamabank:123llama123@margffoy-tuay.com:5672'):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        self._listeners = {}
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self.logger = logger
        self.publisher = publisher
        self._outdb = bancandes.BancAndes.dar_instancia()
        self._outdb.inicializar_ruta('data/connection')

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection == established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        self.logger.info('Connecting to %s', self._url)
        cred = pika.PlainCredentials('llamabank', '123llama123')
        param = pika.ConnectionParameters(
            host='margffoy-tuay.com',
            port=5672,
            virtual_host='bancandesh',
            credentials=cred
        )
        self._connection = adapters.TornadoConnection(param,
                                          self.on_connection_open)

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        self.logger.info('Closing connection')
        self._connection.close()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        self.logger.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method == invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it == unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self.logger.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def on_connection_open(self, unused_connection):
        """This method == called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        self.logger.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        if not self._closing:

            # Create a new connection
            self._connection = self.connect()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        self.logger.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        self.logger.warning('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self._connection.close()

    def on_channel_open(self, channel):
        """This method == invoked by pika when the channel has been opened.
        The channel object == passed in so we can make use of it.

        Since the channel == now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        self.logger.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it == complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        self.logger.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        self.logger.info('Exchange declared')
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it == complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        self.logger.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command == complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        self.logger.info('Binding %s to %s with %s',
                    self.EXCHANGE, self.QUEUE, self.ROUTING_KEY)
        self._channel.queue_bind(self.on_bindok, self.QUEUE,
                                 self.EXCHANGE, self.ROUTING_KEY)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        self.logger.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        self.logger.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        self.logger.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def add_listener(self, listener):
        random_seq = hashlib.md5(str(time.time())).hexdigest()[0:7]
        self._listeners[random_seq] = listener
        return random_seq

    def remove_listener(self, sec):
        del self._listeners[sec]

    def init_transaction(self, msg):
        print msg
        op_type = 3
        if msg['tipo'] == "consignar":
            op_type = 4

        can = self._outdb.verificar_transaccion_cuenta(msg['cuentaOrigen'], msg['monto'], op_type)
        print can
        if can:
            self._outdb.inicializar_estado_externo(msg)
            self.publisher.publish_message(msg)
            # self._listeners[msg['id']].notify_client(msg)
        else:
            msg['estado'] = 'error'
            msg['msg'] = "No cuenta con suficientes fondos en la cuenta %d para realizar esta operación" % (msg["cuentaOrigen"])
            self._listeners[msg['id']].notify_client(msg)

    def init_associate(self, msg):
        self._outdb.inicializar_estado_externo(msg)
        self.publisher.publish_message(msg)
        # self._listeners[msg['id']].notify_client(msg)

    def init_pay(self, msg):
        self._outdb.inicializar_estado_externo(msg)
        msg['saldo'] = str(self._outdb.obtener_saldo_cuenta(msg['numCuenta']))
        self.publisher.publish_message(msg)

    def init_operations(self, msg):
        self.publisher.publish_message(msg)




    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message == delivered from RabbitMQ. The
        channel == passed for your convenience. The basic_deliver object that
        == passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in == an
        instance of BasicProperties with the message properties and the body
        == the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        self.acknowledge_message(basic_deliver.delivery_tag)
        self.logger.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        #Aquí inicia el procesamiento!
        #{“estado”: <”comienzo”|”confirmacion”|”error”>, “id”: nId, “tipo”: <“consignar”|”retirar”>, 
        #“monto”: nMonto, “cuentaDestino”: nCDestino, “cuentaOrigen”: nCOrigen}

        #{“estado”: <”comienzo”|”confirmacion”|”error”>“id”: nId, “tipo”: “asociar”, “cuentaOrigen”: nCOrigen, 
        #“bancoOrigen”: <”bancandes”|”llamabank”> “cuentaDestino”: nCDestino, “bancoDestino”:  <”bancandes”|”llamabank”>, 
        #“nombreEmpleado”:nNombre, “valor”: nValor, “frecuencia”:<”mensual”|”quincenal”>}

        dic = json.loads(body)
        print dic
        regCuenta = False
        notify = False

        print dic['estado']
        print dic[u'estado']
        print dic['tipo']
        print dic[u'tipo']

        if dic[u'estado'] == u'comienzo':
            if dic[u'tipo'] == u'consignar':
                tipo = 3
                regCuenta = True
            elif dic[u'tipo'] == u'retirar':
                tipo = 4
                regCuenta = True
            elif dic[u'tipo'] == u'asociar':
                print "empieza proceso de asociar"
                dic[u'estado'] = u'confirmacion'
                cuenta_nat = dic[u'cuentaDestino']
                existe = self._outdb.existe_cuenta(cuenta_nat)
                if not existe:
                    dic[u'estado'] = u'error'
                else:
                    if dic[u'frecuencia'] == u"mensual":
                        frecuencia = 1
                    else:
                        frecuencia = 2

                    ret = self._outdb.actualizar_nomina_ext(dic[u'cuentaOrigen'], cuenta_nat, dic['valor'], frecuencia)

                    if not ret[0]:
                        dic[u'estado'] = u'error'
                print ("justo antes de enviar mensaje")
                self.publisher.publish_message(dic)
            elif dic[u'tipo'] == u'pagar':
                print "empieza proceso de pago"
                cuentaJ = dic[u'numCuenta']
                saldo = dic [u'saldo']
                ok, _list, saldo = self._outdb.pagar_nomina_ext(cuentaJ, saldo)
                dic['saldo'] = str(saldo)
                if not ok:
                    dic['estado'] = 'error'
                    dic['cuentas'] = _list
                else:
                    dic['estado'] = 'confirmacion'
                self.publisher.publish_message(dic)


            elif dic[u'tipo'] == u'consultaOp':
                params = {'client':False, 'account':False, 'loan':False, 
                      'op_type':-1,
                      'last_movement':[self.from_timestamp(str(dic['fechaIni'])),self.from_timestamp(str(dic['fechaFin']))],
                      'sum':[dic['valorMin'], dic['valorMax']],
                      'pa':[None, None],
                      'search_term':"",
                      'negate':False}
                perm = {'ggeneral':True, 'goficina':False, 'cliente':False}
                search_count, count, cuentas = self._outdb.obtener_operacionL('numero','asc', 0, 100, perm, params, None)
                cuentas = map(lambda x: x.dict_repr(), cuentas)
                cuentas = [dict(zip(j.keys(), [str(j[i]) for i in j])) for j in cuentas]
                dic['operaciones'] = cuentas
                dic[u'tipo'] = u'respuestaOp'
                self.publisher.publish_message(dic)
                


            elif dic[u'tipo'] == u'respuestaOp':
                self._listeners[dic['id']].notify_client(dic)

            elif dic[u'tipo'] == u'consultaPA':
                 params = {'client':False, 'account':False, 'loan':False, 
                      'op_type':-1,
                      'last_movement':[None, None],
                      'sum':[None, None],
                      'pa':[dic['punto1'], dic['punto2']],
                      'search_term':"",
                      'negate':False}
                 perm = {'ggeneral':True, 'goficina':False, 'cliente':False}
                 search_count, count, cuentas = self._outdb.obtener_operacionL('numero','asc', 0, 100, perm, params, None)
                 cuentas = map(lambda x: x.dict_repr(), cuentas)
                 cuentas = [dict(zip(j.keys(), [str(j[i]) for i in j])) for j in cuentas]
                 dic['pas'] = cuentas
                 dic[u'tipo'] = u'respuestaOp' 
                 self.publisher.publish_message(dic)


            elif dic[u'tipo'] == u'respuestaPA':
                dic['operaciones'] = dic['pas']
                self._listeners[dic['id']].notify_client(dic)
                



            if regCuenta:
                print ("empieza proceso transferencia de cuentas")
                numero = self._outdb.generar_numero_operacion()
                idPuntoAtencion = '3'
                cajero = 'NULL'
                numeroCuenta = dic[u'cuentaDestino']
                existe = self._outdb.existe_cuenta(numeroCuenta)
                if existe:
                    cliente = self._outdb.duenio_cuenta(numeroCuenta)
                    monto = dic[u'monto']
                    oper = operacion.Operacion(numero,tipo,cliente,monto,idPuntoAtencion, cajero, numeroCuenta, str(datetime.date.today()))
                    pudo = self._outdb.registrar_operacion_cuenta(oper)
                    if pudo:
                        dic[u'estado'] = u'confirmacion'
                    else:
                        dic[u'estado'] = u'error'
                    print "justo antes de publicar mensaje"
                    self.publisher.publish_message(dic)


        elif dic[u'estado'] == u'confirmacion':
            notify = True
            if dic[u'tipo'] == u'consignar':
                tipo = "3"
                regCuenta = True
            elif dic[u'tipo'] == u'retirar':
                tipo = "4"
                regCuenta = True
            elif dic[u'tipo'] == u'pagar':
                self._outdb.actualizar_saldo_cuenta(dic['numCuenta'], dic['saldo'])


            if regCuenta:
                numero = self._outdb.generar_numero_operacion()
                idPuntoAtencion = '3'
                cajero = 'NULL'
                numeroCuenta = dic[u'cuentaOrigen']
                existe = self._outdb.existe_cuenta(numeroCuenta)
                if existe:
                    cliente = self._outdb.duenio_cuenta(numeroCuenta)
                    monto = dic[u'monto']
                    oper = operacion.Operacion(numero,tipo,cliente,monto,idPuntoAtencion, cajero, numeroCuenta, str(datetime.date.today()))
                    pudo = self._outdb.registrar_operacion_cuenta_externo(oper)
                    print pudo
                    if isinstance(pudo, str):
                        dic[u'estado'] = u'error'
                        dic[u'msg'] = u"Ha ocurrido un error mientras se realizaba la operación"
                        

        elif dic[u'estado'] == u'error':
            dic[u'msg'] = "Ha ocurrido un error mientras se realizaba la operación"
            if dic[u'tipo'] == u'pagar':
                self._outdb.actualizar_saldo_cuenta(dic['numCuenta'], dic['saldo'])

        if notify:
            try:
                self._listeners[dic[u'id']].notify_client(dic)
            except KeyError:
                id_cliente = self._outdb.obtener_id_transaccion(dic[u'id'])
                print id_cliente
                if dic[u'estado'] == u'error':
                    msg = u"La transacción externa con número de confirmación "+dic[u'id']+u" no pudo llevarse a cabo."
                else:
                    msg = u"La transacción externa con número de confirmación "+dic[u'id']+u" se llevó a cabo exitosamente."
                self._outdb.notificar(id_cliente,msg)
            self._outdb.actualizar_estado_externo(dic[u'id'], dic[u'estado'])


        
     

    def on_cancelok(self, unused_frame):
        """This method == invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        self.logger.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            self.logger.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object == notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that == used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method == passed in as a callback pika
        will invoke when a message == fully received.

        """
        self.logger.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.QUEUE)

    def from_timestamp(self, date):
        if date != '':
            date = map(int, date.split('-'))
            return datetime.date(date[0], date[1], date[2])
        else:
            return None

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        self.logger.info('Queue bound')
        self.start_consuming()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        self.logger.info('Closing the channel')
        self._channel.close()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel == open, the
        on_channel_open callback will be invoked by pika.

        """
        self.logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

