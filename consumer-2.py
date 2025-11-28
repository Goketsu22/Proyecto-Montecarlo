# consumer.py
import pika
import json
import time
import sys
from threading import Thread, Event


class ConexionRabbit:
    def __init__(self, host="10.163.238.60", port=5672, usuario="admin", contrasena="admin"):
        self.host = host
        self.puerto = port
        self.usuario = usuario
        self.contrasena = contrasena
        self.conexion = None
        self.canal = None
    def obtener_parametros(self):
        return pika.ConnectionParameters(
            host=self.host,
            port=self.puerto,
            credentials=pika.PlainCredentials(self.usuario, self.contrasena),
            heartbeat=600,
            blocked_connection_timeout=300
        )

    def conectar(self):
        self.conexion = pika.BlockingConnection(self.obtener_parametros())
        self.canal = self.conexion.channel()
        return self.canal

    def cerrar(self):
        if self.conexion and not self.conexion.is_closed:
            self.conexion.close()

    def declarar_cola_segura(self, canal, cola, durable=False):
        try:
            canal.queue_declare(queue=cola, passive=True)
        except pika.exceptions.ChannelClosedByBroker:
            canal = self.conectar()
            canal.queue_declare(queue=cola, durable=durable)
        return canal


class ObtenedorModelo(ConexionRabbit):
    def __init__(self, cola="cola_modelo", **kwargs):
        super().__init__(**kwargs)
        self.cola = cola
        self.version_modelo = None
        self.codigo_modelo = None
        self.funcion_modelo = None

    def obtener_modelo(self, espera_maxima=120):
        inicio = time.time()
        print(f"[TRABAJADOR] Buscando modelo en cola '{self.cola}'...")

        while (time.time() - inicio) < espera_maxima:
            try:
                canal = self.conectar()
                try:
                    info = canal.queue_declare(queue=self.cola, passive=True)
                    if info.method.message_count == 0:
                        self.cerrar()
                        time.sleep(2)
                        continue
                except pika.exceptions.ChannelClosedByBroker:
                    self.cerrar()
                    time.sleep(2)
                    continue

                metodo, _, cuerpo = canal.basic_get(queue=self.cola, auto_ack=False)
                if metodo:
                    datos = json.loads(cuerpo.decode())
                    self.version_modelo = datos.get("version")
                    self.codigo_modelo = datos.get("codigo")
                    canal.basic_nack(delivery_tag=metodo.delivery_tag, requeue=True)
                    self._compilar_modelo()
                    self.cerrar()
                    print(f"[TRABAJADOR] Modelo recibido: {self.version_modelo[:12]}...")
                    return True

                self.cerrar()
                time.sleep(2)
            except Exception as e:
                print(f"[ERROR] Conexion fallida: {e}")
                self.cerrar()
                time.sleep(3)

        print(f"[ERROR] Timeout esperando modelo")
        return False

    def _compilar_modelo(self):
        if not self.codigo_modelo:
            raise ValueError("No hay codigo de modelo")
        
        espacio = {}
        exec(self.codigo_modelo, espacio)
        self.funcion_modelo = espacio.get("model_fn")
        
        if not self.funcion_modelo:
            raise ValueError("El modelo no contiene 'model_fn'")
        
        self.funcion_modelo({"prueba": True})
        print("[TRABAJADOR] Modelo compilado y verificado")


class OyenteActualizaciones(ConexionRabbit):
    def __init__(self, cola="actualizaciones_modelo", **kwargs):
        super().__init__(**kwargs)
        self.cola = cola
        self.evento_actualizacion = Event()
        self.nueva_version = None
        self.ejecutando = True

    def iniciar_escucha(self):
        Thread(target=self._bucle_escucha, daemon=True).start()
        print("[TRABAJADOR] Escuchando actualizaciones de modelo...")

    def _bucle_escucha(self):
        while self.ejecutando:
            try:
                conexion = pika.BlockingConnection(self.obtener_parametros())
                canal = conexion.channel()
                
                try:
                    canal.queue_declare(queue=self.cola, passive=True)
                except pika.exceptions.ChannelClosedByBroker:
                    canal = conexion.channel()
                    canal.queue_declare(queue=self.cola, durable=False)

                def callback(ch, metodo, props, cuerpo):
                    datos = json.loads(cuerpo.decode())
                    if datos.get("evento") == "modelo_actualizado":
                        self.nueva_version = datos.get("nueva_version")
                        self.evento_actualizacion.set()
                        print(f"\n[TRABAJADOR] Actualizacion detectada: {self.nueva_version[:12]}...")
                    ch.basic_ack(metodo.delivery_tag)

                canal.basic_consume(queue=self.cola, on_message_callback=callback)
                canal.start_consuming()
            except Exception as e:
                if self.ejecutando:
                    print(f"[ADVERTENCIA] Reconectando oyente: {e}")
                    time.sleep(5)

    def verificar_actualizacion(self):
        if self.evento_actualizacion.is_set():
            self.evento_actualizacion.clear()
            return True, self.nueva_version
        return False, None

    def detener(self):
        self.ejecutando = False


class Publicador(ConexionRabbit):
    def __init__(self, cola, **kwargs):
        super().__init__(**kwargs)
        self.cola = cola

    def publicar(self, datos):
        try:
            canal = self.conectar()
            canal = self.declarar_cola_segura(canal, self.cola)
            canal.basic_publish(
                exchange='',
                routing_key=self.cola,
                body=json.dumps(datos).encode(),
                properties=pika.BasicProperties(content_type='application/json')
            )
            self.cerrar()
        except Exception as e:
            print(f"[ERROR] No se pudo publicar en {self.cola}: {e}")


class TrabajadorMonteCarlo:
    CONFIG = {
        "host": "10.163.238.60",
        "port": 5672,
        "usuario": "admin",
        "contrasena": "admin"
    }

    def __init__(self, id_consumidor):
        self.id_consumidor = id_consumidor
        self.obtenedor_modelo = ObtenedorModelo(**self.CONFIG)
        self.publicador_resultados = Publicador(cola="resultados", **self.CONFIG)
        self.publicador_estadisticas = Publicador(cola="estadisticas", **self.CONFIG)
        self.oyente_actualizaciones = OyenteActualizaciones(**self.CONFIG)
        
        self.contador_procesados = 0
        self.contador_errores = 0
        self.tiempo_inicio = None
        self.ultimo_tiempo_stats = time.time()
        self.version_modelo = None

    def inicializar(self):
        print(f"\n{'=' * 60}")
        print(f"[TRABAJADOR {self.id_consumidor}] INICIALIZANDO")
        print(f"{'=' * 60}")
        print(f"Servidor: {self.CONFIG['host']}:{self.CONFIG['port']}")

        if self.obtenedor_modelo.obtener_modelo():
            self.version_modelo = self.obtenedor_modelo.version_modelo
            self.tiempo_inicio = time.time()
            self.oyente_actualizaciones.iniciar_escucha()
            print(f"[TRABAJADOR {self.id_consumidor}] Listo")
            return True
        
        print(f"[TRABAJADOR {self.id_consumidor}] Error al inicializar")
        return False

    def recargar_modelo(self):
        print(f"[TRABAJADOR {self.id_consumidor}] Recargando modelo...")
        if self.obtenedor_modelo.obtener_modelo(espera_maxima=30):
            if self.obtenedor_modelo.version_modelo != self.version_modelo:
                self.version_modelo = self.obtenedor_modelo.version_modelo
                print(f"[TRABAJADOR {self.id_consumidor}] Modelo actualizado")
                return True
        return False

    def procesar_escenario(self, escenario):
        inicio = time.time()
        try:
            version_escenario = escenario.get("version_modelo")
            if version_escenario and version_escenario != self.version_modelo:
                self.recargar_modelo()
            
            resultado = self.obtenedor_modelo.funcion_modelo(escenario)
            exito = True
        except Exception as e:
            resultado = {"error": str(e)}
            exito = False
            self.contador_errores += 1

        self.contador_procesados += 1
        return {
            "consumidor": self.id_consumidor,
            "id_escenario": escenario["id"],
            "resultado": resultado,
            "marca_tiempo": time.time(),
            "tiempo_procesamiento": time.time() - inicio,
            "exito": exito,
            "version_modelo": self.version_modelo
        }

    def publicar_estadisticas(self, forzar=False):
        if forzar or (time.time() - self.ultimo_tiempo_stats) >= 30:
            tiempo_activo = time.time() - self.tiempo_inicio if self.tiempo_inicio else 0
            self.publicador_estadisticas.publicar({
                "consumidor": self.id_consumidor,
                "procesados": self.contador_procesados,
                "errores": self.contador_errores,
                "tiempo_activo": tiempo_activo,
                "tasa": self.contador_procesados / tiempo_activo if tiempo_activo > 0 else 0,
                "version_modelo": self.version_modelo,
                "marca_tiempo": time.time()
            })
            self.ultimo_tiempo_stats = time.time()

    def iniciar_consumo(self, cola_escenarios="escenarios"):
        params = pika.ConnectionParameters(
            host=self.CONFIG["host"],
            port=self.CONFIG["port"],
            credentials=pika.PlainCredentials(self.CONFIG["usuario"], self.CONFIG["contrasena"]),
            heartbeat=600,
            blocked_connection_timeout=300
        )

        for intento in range(3):
            try:
                conexion = pika.BlockingConnection(params)
                canal = conexion.channel()
                
                try:
                    canal.queue_declare(queue=cola_escenarios, passive=True)
                except pika.exceptions.ChannelClosedByBroker:
                    conexion.close()
                    conexion = pika.BlockingConnection(params)
                    canal = conexion.channel()
                    canal.queue_declare(
                        queue=cola_escenarios,
                        durable=True,
                        arguments={
                            'x-max-length': 50000,
                            'x-overflow': 'reject-publish',
                            'x-message-ttl': 3600000,
                            'x-queue-mode': 'lazy'
                        }
                    )
                break
            except Exception as e:
                print(f"[TRABAJADOR {self.id_consumidor}] Intento {intento + 1} fallido: {e}")
                if intento == 2:
                    raise
                time.sleep(2)

        canal.basic_qos(prefetch_count=1)

        def al_recibir(ch, metodo, props, cuerpo):
            hay_actualizacion, _ = self.oyente_actualizaciones.verificar_actualizacion()
            if hay_actualizacion:
                self.recargar_modelo()

            escenario = json.loads(cuerpo.decode())
            resultado = self.procesar_escenario(escenario)
            self.publicador_resultados.publicar(resultado)

            if self.contador_procesados % 50 == 0:
                transcurrido = time.time() - self.tiempo_inicio
                tasa = self.contador_procesados / transcurrido
                print(f"[TRABAJADOR {self.id_consumidor}] Procesados: {self.contador_procesados} | Tasa: {tasa:.1f}/s")

            self.publicar_estadisticas()
            ch.basic_ack(metodo.delivery_tag)

        canal.basic_consume(queue=cola_escenarios, on_message_callback=al_recibir)

        print(f"\n{'=' * 60}")
        print(f"[TRABAJADOR {self.id_consumidor}] CONSUMIDOR ACTIVO")
        print(f"{'=' * 60}")
        print(f"Cola: {cola_escenarios}")
        print(f"Modelo: {self.version_modelo[:12]}...")
        print(f"Esperando escenarios... (Ctrl+C para detener)")

        try:
            canal.start_consuming()
        except KeyboardInterrupt:
            print(f"\n[TRABAJADOR {self.id_consumidor}] Interrumpido")
        finally:
            self.oyente_actualizaciones.detener()
            self.publicar_estadisticas(forzar=True)
            self._mostrar_resumen()
            try:
                canal.stop_consuming()
                conexion.close()
            except:
                pass

    def _mostrar_resumen(self):
        print(f"\n{'=' * 60}")
        print(f"[TRABAJADOR {self.id_consumidor}] RESUMEN FINAL")
        print(f"{'=' * 60}")
        if self.tiempo_inicio:
            tiempo_total = time.time() - self.tiempo_inicio
            print(f"  Tiempo activo: {tiempo_total:.2f}s")
            print(f"  Procesados: {self.contador_procesados}")
            print(f"  Tasa: {self.contador_procesados / tiempo_total:.2f}/s")
            print(f"  Errores: {self.contador_errores}")


def main():
    id_consumidor = sys.argv[1] if len(sys.argv) > 1 else f"trabajador-{int(time.time())}"
    
    trabajador = TrabajadorMonteCarlo(id_consumidor)
    
    print(f"\nCONSUMIDOR INICIADO")
    print(f"ID: {id_consumidor}")
    
    if trabajador.inicializar():
        trabajador.iniciar_consumo()
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()