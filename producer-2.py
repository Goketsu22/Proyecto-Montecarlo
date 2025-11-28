# producer.py
import pika
import json
import time
import uuid
import random
import numpy as np
from threading import Thread, Event


class CodificadorNumpy(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int32, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


class ConexionRabbit:
    def __init__(self, host="10.163.238.60", port=5672, usuario="admin", contrasena="admin"):
        self.host = host
        self.puerto = port
        self.usuario = usuario
        self.contrasena = contrasena
        self.conexion = None
        self.canal = None

    def conectar(self):
        credenciales = pika.PlainCredentials(self.usuario, self.contrasena)
        parametros = pika.ConnectionParameters(
            host=self.host,
            port=self.puerto,
            credentials=credenciales,
            heartbeat=600,
            blocked_connection_timeout=300
        )
        self.conexion = pika.BlockingConnection(parametros)
        self.canal = self.conexion.channel()
        return self.canal

    def cerrar(self):
        if self.conexion and not self.conexion.is_closed:
            self.conexion.close()

    def publicar_mensaje(self, cola, mensaje, persistente=False, ttl=None):
        canal = self.conectar()
        propiedades = pika.BasicProperties(
            delivery_mode=2 if persistente else 1,
            content_type='application/json'
        )
        if ttl:
            propiedades.expiration = str(ttl)
        
        canal.basic_publish(
            exchange='',
            routing_key=cola,
            body=json.dumps(mensaje).encode() if isinstance(mensaje, dict) else mensaje,
            properties=propiedades
        )
        self.cerrar()


class PublicadorModelo(ConexionRabbit):
    def __init__(self, cola="cola_modelo", ttl=300000, **kwargs):
        super().__init__(**kwargs)
        self.cola = cola
        self.ttl = ttl
        self.version_modelo = None

    def publicar_modelo(self, texto_modelo):
        version_anterior = self.version_modelo
        self.version_modelo = str(uuid.uuid4())
        
        canal = self.conectar()
        
        if version_anterior:
            try:
                canal.queue_delete(queue=self.cola)
                print(f"[PRODUCTOR] Modelo anterior invalidado")
                time.sleep(0.3)
            except Exception:
                pass

        canal.queue_declare(
            queue=self.cola,
            durable=False,
            arguments={'x-message-ttl': self.ttl, 'x-max-length': 1}
        )

        datos_modelo = {
            "version": self.version_modelo,
            "marca_tiempo": time.time(),
            "codigo": texto_modelo,
            "estado": "activo"
        }

        canal.basic_publish(
            exchange='',
            routing_key=self.cola,
            body=json.dumps(datos_modelo).encode(),
            properties=pika.BasicProperties(
                delivery_mode=1,
                expiration=str(self.ttl),
                headers={'version-modelo': self.version_modelo}
            )
        )

        self.cerrar()
        print(f"[PRODUCTOR] Nuevo modelo publicado - Version: {self.version_modelo[:12]}...")
        return self.version_modelo


class Notificador(ConexionRabbit):
    def __init__(self, cola, **kwargs):
        super().__init__(**kwargs)
        self.cola = cola

    def notificar(self, mensaje):
        canal = self.conectar()
        canal.queue_declare(queue=self.cola, durable=False)
        canal.basic_publish(
            exchange='',
            routing_key=self.cola,
            body=json.dumps(mensaje).encode()
        )
        self.cerrar()


class ProductorEscenariosContinuo(ConexionRabbit):
    def __init__(self, cola="escenarios", escenarios_minimos=1000, 
                 escenarios_maximos=50000, tamano_lote=500, **kwargs):
        super().__init__(**kwargs)
        self.cola = cola
        self.escenarios_minimos = escenarios_minimos
        self.escenarios_maximos = escenarios_maximos
        self.tamano_lote = tamano_lote
        self.esta_ejecutando = False
        self.hilo_productor = None
        self.evento_detener = Event()
        self.escenarios_publicados = 0
        self.version_modelo_actual = None
        self._configurar_cola()

    def _configurar_cola(self):
        try:
            canal = self.conectar()
            canal.queue_declare(
                queue=self.cola,
                durable=True,
                arguments={
                    'x-max-length': self.escenarios_maximos,
                    'x-overflow': 'reject-publish',
                    'x-message-ttl': 3600000,
                    'x-queue-mode': 'lazy'
                }
            )
            self.cerrar()
            print(f"[PRODUCTOR] Cola '{self.cola}' configurada")
        except Exception as e:
            print(f"[PRODUCTOR] Error configurando cola: {e}")

    def establecer_version_modelo(self, version):
        self.version_modelo_actual = version

    def _generar_distribuciones(self):
        return {
            "uniforme": random.random(),
            "uniforme_rango": random.uniform(0, 100),
            "normal_estandar": random.gauss(0, 1),
            "normal_personalizada": random.gauss(50, 15),
            "binomial": np.random.binomial(100, 0.5),
            "poisson": np.random.poisson(10),
            "bernoulli": np.random.binomial(1, 0.3),
            "exponencial": np.random.exponential(2),
            "gamma": np.random.gamma(2, 2),
            "beta": np.random.beta(2, 5),
        }

    def _generar_escenario(self, indice):
        distribuciones = self._generar_distribuciones()
        return {
            "id": str(uuid.uuid4()),
            "indice": indice,
            "marca_tiempo": time.time(),
            "version_modelo": self.version_modelo_actual,
            "distribuciones": distribuciones,
            "metadatos": {
                "marca_tiempo_lote": int(time.time()),
                "tipos_distribucion": list(distribuciones.keys())
            }
        }

    def _obtener_estado_cola(self):
        try:
            canal = self.conectar()
            info = canal.queue_declare(queue=self.cola, passive=True)
            resultado = {
                "cantidad_mensajes": info.method.message_count,
                "cantidad_consumidores": info.method.consumer_count,
                "necesita_mas": info.method.message_count < self.escenarios_minimos
            }
            self.cerrar()
            return resultado
        except Exception:
            return {"cantidad_mensajes": 0, "cantidad_consumidores": 0, "necesita_mas": True}

    def _publicar_lote(self, cantidad):
        if cantidad <= 0:
            return 0
        try:
            canal = self.conectar()
            for i in range(cantidad):
                escenario = self._generar_escenario(self.escenarios_publicados + i)
                canal.basic_publish(
                    exchange='',
                    routing_key=self.cola,
                    body=json.dumps(escenario, cls=CodificadorNumpy).encode(),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type='application/json'
                    )
                )
            self.cerrar()
            self.escenarios_publicados += cantidad
            return cantidad
        except Exception as e:
            print(f"[PRODUCTOR] Error publicando lote: {e}")
            return 0

    def _ciclo_produccion(self):
        while not self.evento_detener.is_set():
            try:
                estado = self._obtener_estado_cola()
                cantidad_actual = estado["cantidad_mensajes"]
                
                if cantidad_actual < self.escenarios_minimos:
                    necesarios = min(self.escenarios_minimos - cantidad_actual, self.tamano_lote)
                    if estado["cantidad_consumidores"] > 0 and cantidad_actual < (self.escenarios_minimos // 2):
                        necesarios = min(necesarios * 2, self.tamano_lote * 2)
                    
                    if necesarios > 0:
                        generados = self._publicar_lote(necesarios)
                        if generados > 0:
                            print(f"[PRODUCTOR] +{generados} escenarios | Total: {self.escenarios_publicados}")

                tiempo_espera = 0.5 if cantidad_actual < (self.escenarios_minimos // 2) else 3.0
                time.sleep(tiempo_espera)
            except Exception as e:
                print(f"[PRODUCTOR] Error en ciclo: {e}")
                time.sleep(10)

    def iniciar_produccion(self):
        if self.esta_ejecutando:
            return False
        
        print(f"[PRODUCTOR] Iniciando produccion continua")
        self.esta_ejecutando = True
        self.evento_detener.clear()
        
        estado = self._obtener_estado_cola()
        if estado["cantidad_mensajes"] < self.escenarios_minimos:
            self._publicar_lote(self.escenarios_minimos - estado["cantidad_mensajes"])
        
        self.hilo_productor = Thread(target=self._ciclo_produccion, daemon=True)
        self.hilo_productor.start()
        return True

    def detener_produccion(self):
        if not self.esta_ejecutando:
            return
        self.esta_ejecutando = False
        self.evento_detener.set()
        if self.hilo_productor and self.hilo_productor.is_alive():
            self.hilo_productor.join(timeout=10)
        print(f"[PRODUCTOR] Detenido - Total publicados: {self.escenarios_publicados}")

    def obtener_estado_detallado(self):
        estado_cola = self._obtener_estado_cola()
        return {
            "produccion": {
                "esta_ejecutando": self.esta_ejecutando,
                "total_publicados": self.escenarios_publicados,
                "modelo_actual": self.version_modelo_actual
            },
            "cola": estado_cola,
            "limites": {
                "escenarios_minimos": self.escenarios_minimos,
                "escenarios_maximos": self.escenarios_maximos,
                "tamano_lote": self.tamano_lote
            }
        }


class ProductorMonteCarloContinuo:
    CONFIG_RABBIT = {
        "host": "10.163.238.60",
        "port": 5672,
        "usuario": "admin",
        "contrasena": "admin"
    }

    def __init__(self, archivo_modelo="trafico.txt", escenarios_minimos=2000, escenarios_maximos=50000):
        self.archivo_modelo = archivo_modelo
        self.version_modelo_actual = None
        
        self.publicador_modelo = PublicadorModelo(**self.CONFIG_RABBIT)
        self.notificador_actualizaciones = Notificador(cola="actualizaciones_modelo", **self.CONFIG_RABBIT)
        self.notificador_dashboard = Notificador(cola="dashboard_actualizaciones", **self.CONFIG_RABBIT)
        self.productor_escenarios = ProductorEscenariosContinuo(
            escenarios_minimos=escenarios_minimos,
            escenarios_maximos=escenarios_maximos,
            tamano_lote=1000,
            **self.CONFIG_RABBIT
        )

    def cargar_modelo(self):
        try:
            with open(self.archivo_modelo, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            print(f"[ERROR] Archivo {self.archivo_modelo} no encontrado")
            return None

    def inicializar_sistema(self):
        print("=" * 60)
        print("INICIALIZANDO SISTEMA DE SIMULACION CONTINUA")
        print("=" * 60)
        
        texto_modelo = self.cargar_modelo()
        if not texto_modelo:
            return False
        
        self.version_modelo_actual = self.publicador_modelo.publicar_modelo(texto_modelo)
        self.productor_escenarios.establecer_version_modelo(self.version_modelo_actual)
        self.productor_escenarios.iniciar_produccion()
        
        print("Sistema inicializado correctamente")
        return True

    def actualizar_modelo(self, nuevo_archivo=None):
        if nuevo_archivo:
            self.archivo_modelo = nuevo_archivo
        
        print("\n" + "=" * 60)
        print("ACTUALIZANDO MODELO")
        print("=" * 60)
        
        texto_modelo = self.cargar_modelo()
        if not texto_modelo:
            return False
        
        nueva_version = self.publicador_modelo.publicar_modelo(texto_modelo)
        self.productor_escenarios.establecer_version_modelo(nueva_version)
        
        self.notificador_dashboard.notificar({
            "evento": "modelo_cambiado",
            "nueva_version": nueva_version,
            "archivo_modelo": self.archivo_modelo,
            "marca_tiempo": time.time()
        })
        
        self.notificador_actualizaciones.notificar({
            "evento": "modelo_actualizado",
            "nueva_version": nueva_version,
            "marca_tiempo": time.time()
        })
        
        self.version_modelo_actual = nueva_version
        print(f"Modelo actualizado: {nueva_version[:12]}...")
        return True

    def ejecutar_consola_gestion(self):
        if not self.inicializar_sistema():
            return
        
        try:
            while True:
                print("\n" + "-" * 40)
                print("CONSOLA DE GESTION")
                print("-" * 40)
                print("1. Estado del sistema")
                print("2. Actualizar modelo")
                print("3. Estadisticas detalladas")
                print("4. Salir")
                
                opcion = input("\nOpcion: ").strip()
                
                if opcion == "1":
                    self._mostrar_estado()
                elif opcion == "2":
                    nuevo = input("Archivo del modelo (Enter para mismo): ").strip()
                    self.actualizar_modelo(nuevo if nuevo else None)
                elif opcion == "3":
                    self._mostrar_estadisticas()
                elif opcion == "4":
                    break
        except KeyboardInterrupt:
            print("\nInterrumpido por usuario")
        finally:
            self.productor_escenarios.detener_produccion()

    def _mostrar_estado(self):
        estado = self.productor_escenarios.obtener_estado_detallado()
        print(f"\nESTADO DEL SISTEMA:")
        print(f"  Modelo: {estado['produccion']['modelo_actual'][:12]}...")
        print(f"  Produccion: {'ACTIVA' if estado['produccion']['esta_ejecutando'] else 'DETENIDA'}")
        print(f"  En cola: {estado['cola']['cantidad_mensajes']}")
        print(f"  Consumidores: {estado['cola']['cantidad_consumidores']}")

    def _mostrar_estadisticas(self):
        estado = self.productor_escenarios.obtener_estado_detallado()
        print(f"\nESTADISTICAS:")
        print(f"  Minimo: {estado['limites']['escenarios_minimos']}")
        print(f"  Maximo: {estado['limites']['escenarios_maximos']}")
        print(f"  En cola: {estado['cola']['cantidad_mensajes']}")
        print(f"  Total publicados: {estado['produccion']['total_publicados']}")


if __name__ == "__main__":
    productor = ProductorMonteCarloContinuo(
        archivo_modelo="trafico.txt",
        escenarios_minimos=2000,
        escenarios_maximos=50000
    )
    
    print("\n" + "=" * 60)
    print("SISTEMA DE SIMULACION MONTECARLO - PRODUCCION CONTINUA")
    print("=" * 60)
    print(f"Servidor: {productor.CONFIG_RABBIT['host']}:{productor.CONFIG_RABBIT['port']}")
    print(f"Usuario: {productor.CONFIG_RABBIT['usuario']}")
    
    productor.ejecutar_consola_gestion()