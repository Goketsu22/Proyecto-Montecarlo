# dashboard.py
import eventlet
eventlet.monkey_patch()

from flask import Flask, render_template_string
from flask_socketio import SocketIO
import threading
import json
import time
import pika
from collections import defaultdict, deque


class MetricasDashboard:
    def __init__(self):
        self._inicializar_estado()

    def _inicializar_estado(self):
        self.total_procesados = 0
        self.total_errores = 0
        self.consumidores_activos = set()
        self.estadisticas_consumidor = {}
        self.ids_procesados = set()
        self.carga_trabajo_consumidor = defaultdict(int)
        self.errores_consumidor = defaultdict(int)
        self.historial_resultados = deque(maxlen=1000)
        self.metricas_descubiertas = set()
        self.tipos_metricas = {}
        self.agregados_numericos = defaultdict(list)
        self.tiempo_inicio = None
        self.ultimo_tiempo_resultado = None
        self.esta_terminado = False
        self.umbral_terminacion = 5
        self.info_modelo = {
            "estado_cola": "Desconocido",
            "cantidad_mensajes": 0,
            "ttl_segundos": 300,
            "ultima_actualizacion": None,
            "politica": "Caducidad al cargar nuevo modelo",
            "version": "Desconocida",
            "archivo_actual": "Desconocido"
        }

    def reiniciar_metricas(self):
        print("[DASHBOARD] Reiniciando metricas para nuevo modelo...")
        self._inicializar_estado()
        print("[DASHBOARD] Metricas reiniciadas correctamente")

    def actualizar_info_modelo(self, estado_cola, cantidad_mensajes, version=None, archivo=None):
        self.info_modelo.update({
            "estado_cola": estado_cola,
            "cantidad_mensajes": cantidad_mensajes,
            "ultima_actualizacion": time.time(),
            "version": version or self.info_modelo["version"],
            "archivo_actual": archivo or self.info_modelo["archivo_actual"]
        })

    def _descubrir_y_procesar_resultado(self, resultado):
        if not isinstance(resultado, dict):
            return
        for clave, valor in resultado.items():
            self.metricas_descubiertas.add(clave)
            if isinstance(valor, (int, float)):
                self.tipos_metricas[clave] = 'numerica'
                self.agregados_numericos[clave].append(float(valor))

    def actualizar_resultado(self, datos_resultado):
        id_escenario = datos_resultado.get("id_escenario")
        if id_escenario and id_escenario in self.ids_procesados:
            return False

        if self.tiempo_inicio is None:
            self.tiempo_inicio = time.time()

        self.ultimo_tiempo_resultado = time.time()
        self.esta_terminado = False

        if id_escenario:
            self.ids_procesados.add(id_escenario)

        self.total_procesados += 1
        consumidor = datos_resultado.get("consumidor")
        
        if consumidor:
            self.consumidores_activos.add(consumidor)
            self.carga_trabajo_consumidor[consumidor] += 1
            if not datos_resultado.get("exito", True):
                self.errores_consumidor[consumidor] += 1

        if not datos_resultado.get("exito", True):
            self.total_errores += 1

        self._descubrir_y_procesar_resultado(datos_resultado.get("resultado", {}))
        self.historial_resultados.append({
            "marca_tiempo": self.ultimo_tiempo_resultado,
            "consumidor": consumidor,
            "exito": datos_resultado.get("exito", True),
            "resultado": datos_resultado.get("resultado", {})
        })
        return True

    def actualizar_estadisticas(self, datos_estadisticas):
        consumidor = datos_estadisticas.get("consumidor")
        if consumidor:
            self.estadisticas_consumidor[consumidor] = datos_estadisticas
            self.consumidores_activos.add(consumidor)

    def verificar_si_termino(self):
        if self.ultimo_tiempo_resultado is None:
            return False
        transcurrido = time.time() - self.ultimo_tiempo_resultado
        if transcurrido > self.umbral_terminacion and not self.esta_terminado:
            self.esta_terminado = True
            return True
        return self.esta_terminado

    def obtener_rendimiento_consumidor(self):
        rendimiento = {}
        for consumidor in self.consumidores_activos:
            procesados = self.carga_trabajo_consumidor.get(consumidor, 0)
            errores = self.errores_consumidor.get(consumidor, 0)
            tasa_exito = ((procesados - errores) / procesados * 100) if procesados > 0 else 0
            rendimiento[consumidor] = {
                "procesados": procesados,
                "errores": errores,
                "tasa_exito": tasa_exito
            }
        return rendimiento

    def obtener_resumen(self):
        return {
            "total_procesados": self.total_procesados,
            "total_errores": self.total_errores,
            "consumidores_activos": len(self.consumidores_activos),
            "estadisticas_consumidor": self.estadisticas_consumidor,
            "rendimiento_consumidor": self.obtener_rendimiento_consumidor(),
            "carga_trabajo_consumidor": dict(self.carga_trabajo_consumidor),
            "metricas_descubiertas": list(self.metricas_descubiertas),
            "tipos_metricas": self.tipos_metricas,
            "esta_terminado": self.esta_terminado,
            "info_modelo": self.info_modelo
        }


class OyenteRabbitMonteCarlo:
    COLA_DASHBOARD = "dashboard_actualizaciones"
    COLA_MODELO = "cola_modelo"
    COLA_RESULTADOS = "resultados"
    COLA_ESTADISTICAS = "estadisticas"

    def __init__(self, host, port, usuario, contrasena, socketio, metricas):
        self.host = host
        self.puerto = port
        self.usuario = usuario
        self.contrasena = contrasena
        self.socketio = socketio
        self.metricas = metricas
        self.ejecutando = True

    def _obtener_parametros_conexion(self):
        return pika.ConnectionParameters(
            host=self.host,
            port=self.puerto,
            credentials=pika.PlainCredentials(self.usuario, self.contrasena),
            heartbeat=30,
            connection_attempts=3,
            retry_delay=2
        )

    def _verificar_cola_modelo(self):
        try:
            conexion = pika.BlockingConnection(self._obtener_parametros_conexion())
            canal = conexion.channel()
            try:
                cola_modelo = canal.queue_declare(queue=self.COLA_MODELO, passive=True)
                cantidad = cola_modelo.method.message_count
                
                if cantidad > 0:
                    metodo, _, cuerpo = canal.basic_get(queue=self.COLA_MODELO, auto_ack=False)
                    if metodo:
                        try:
                            datos = json.loads(cuerpo.decode())
                            version = datos.get("version", "Desconocida")[:8] + "..."
                            self.metricas.actualizar_info_modelo("Activa", cantidad, version)
                        except:
                            self.metricas.actualizar_info_modelo("Activa", cantidad)
                        canal.basic_nack(metodo.delivery_tag, requeue=True)
                else:
                    self.metricas.actualizar_info_modelo("Activa", cantidad)
            except pika.exceptions.ChannelClosedByBroker:
                self.metricas.actualizar_info_modelo("No existe", 0)
            conexion.close()
        except Exception as e:
            self.metricas.actualizar_info_modelo(f"Error: {str(e)}", 0)

    def _procesar_resultado(self, datos):
        self.metricas.actualizar_resultado(datos)
        self.socketio.emit('resultado', datos)
        resumen = self.metricas.obtener_resumen()
        self.socketio.emit('actualizacion_metricas', resumen)
        if self.metricas.verificar_si_termino():
            self.socketio.emit('simulacion_terminada', resumen)

    def _procesar_estadisticas(self, datos):
        self.metricas.actualizar_estadisticas(datos)
        self.socketio.emit('estadisticas', datos)

    def _procesar_cambio_modelo(self, datos):
        if datos.get("evento") != "modelo_cambiado":
            return
        
        print(f"\n[DASHBOARD] Cambio de modelo detectado!")
        print(f"[DASHBOARD] Nueva version: {datos.get('nueva_version')}")
        print(f"[DASHBOARD] Archivo: {datos.get('archivo_modelo')}")
        
        self.metricas.reiniciar_metricas()
        self.metricas.actualizar_info_modelo(
            "Activa", 1,
            datos.get('nueva_version'),
            datos.get('archivo_modelo')
        )
        self.socketio.emit('modelo_actualizado', {
            "nueva_version": datos.get("nueva_version"),
            "archivo_modelo": datos.get("archivo_modelo"),
            "marca_tiempo": datos.get("marca_tiempo"),
            "mensaje": "Metricas reiniciadas para nuevo modelo"
        })
        print(f"[DASHBOARD] Metricas reiniciadas para nuevo modelo")

    def ejecutar(self):
        print(f"[RABBITMQ] Iniciando oyente en {self.host}:{self.puerto}")
        print(f"[RABBITMQ] Escuchando cambios de modelo...")
        self._verificar_cola_modelo()

        while self.ejecutando:
            conexion = None
            try:
                conexion = pika.BlockingConnection(self._obtener_parametros_conexion())
                canal = conexion.channel()

                canal.queue_declare(queue=self.COLA_RESULTADOS, durable=True)
                canal.queue_declare(queue=self.COLA_ESTADISTICAS, durable=False)
                canal.queue_declare(queue=self.COLA_DASHBOARD, durable=False)

                def callback_resultado(ch, metodo, props, cuerpo):
                    try:
                        self._procesar_resultado(json.loads(cuerpo.decode()))
                        ch.basic_ack(metodo.delivery_tag)
                    except Exception as e:
                        print(f"[DASHBOARD] Error procesando resultado: {e}")

                def callback_estadisticas(ch, metodo, props, cuerpo):
                    try:
                        self._procesar_estadisticas(json.loads(cuerpo.decode()))
                        ch.basic_ack(metodo.delivery_tag)
                    except Exception as e:
                        print(f"[DASHBOARD] Error procesando estadisticas: {e}")

                def callback_dashboard(ch, metodo, props, cuerpo):
                    try:
                        self._procesar_cambio_modelo(json.loads(cuerpo.decode()))
                        ch.basic_ack(metodo.delivery_tag)
                    except Exception as e:
                        print(f"[DASHBOARD] Error procesando notificacion: {e}")

                canal.basic_consume(queue=self.COLA_RESULTADOS, on_message_callback=callback_resultado)
                canal.basic_consume(queue=self.COLA_ESTADISTICAS, on_message_callback=callback_estadisticas)
                canal.basic_consume(queue=self.COLA_DASHBOARD, on_message_callback=callback_dashboard)

                print("[DASHBOARD] Conectado a RabbitMQ - Esperando mensajes...")
                ultima_verificacion = time.time()

                while self.ejecutando:
                    conexion.process_data_events(time_limit=1.0)
                    if time.time() - ultima_verificacion > 10:
                        self._verificar_cola_modelo()
                        self.socketio.emit('actualizacion_metricas', self.metricas.obtener_resumen())
                        ultima_verificacion = time.time()

            except Exception as e:
                if self.ejecutando:
                    print(f"[ERROR RABBIT] {e}")
                    print("[RABBITMQ] Reintentando conexion en 3 segundos...")
                    time.sleep(3)
            finally:
                if conexion and not conexion.is_closed:
                    try:
                        conexion.close()
                    except:
                        pass


class DashboardMonteCarlo:
    TEMPLATE = r"""
<!doctype html>
<html>
<head>
    <title>Dashboard Universal Monte Carlo</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="//cdnjs.cloudflare.com/ajax/libs/socket.io/4.5.4/socket.io.min.js"></script>
    <style>
        * { box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            margin: 0; padding: 20px; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }
        .contenedor { max-width: 1600px; margin: 0 auto; }
        h2 { 
            color: white; text-align: center; font-size: 32px; margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        .subtitulo {
            text-align: center; color: rgba(255,255,255,0.9);
            font-size: 16px; margin-bottom: 30px;
        }
        .banner-estado { 
            background: linear-gradient(135deg, #4CAF50, #45a049); 
            color: white; padding: 20px; border-radius: 12px; 
            text-align: center; font-size: 20px; font-weight: bold; 
            margin: 20px 0; box-shadow: 0 4px 15px rgba(0,0,0,0.2);
            display: none; animation: deslizarEntrada 0.5s ease-out;
        }
        @keyframes deslizarEntrada {
            from { opacity: 0; transform: translateY(-20px); }
            to { opacity: 1; transform: translateY(0); }
        }
        .banner-estado.mostrar { display: block; }
        .banner-estado.terminado { 
            background: linear-gradient(135deg, #2196F3, #1976D2);
            animation: pulsar 2s infinite;
        }
        .banner-estado.cambio-modelo {
            background: linear-gradient(135deg, #FF9800, #F57C00);
            animation: pulsar 1.5s infinite;
        }
        @keyframes pulsar {
            0%, 100% { transform: scale(1); }
            50% { transform: scale(1.02); }
        }
        .cuadricula { 
            display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); 
            gap: 20px; margin: 20px 0; 
        }
        .tarjeta { 
            background: white; border-radius: 12px; padding: 20px; 
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            transition: transform 0.2s, box-shadow 0.2s;
        }
        .tarjeta:hover {
            transform: translateY(-5px);
            box-shadow: 0 6px 20px rgba(0,0,0,0.15);
        }
        .tarjeta h3 { 
            margin: 0 0 15px 0; color: #333; font-size: 18px;
            border-bottom: 2px solid #667eea; padding-bottom: 10px;
        }
        .valor-metrica { 
            font-size: 36px; font-weight: bold; color: #667eea;
            text-align: center; margin: 15px 0;
        }
        .etiqueta-metrica {
            text-align: center; color: #666; font-size: 14px;
            text-transform: uppercase; letter-spacing: 1px;
        }
        .barra-progreso {
            width: 100%; height: 8px; background: #e0e0e0;
            border-radius: 4px; overflow: hidden; margin: 5px 0;
        }
        .relleno-progreso {
            height: 100%; background: linear-gradient(90deg, #667eea, #764ba2);
            transition: width 0.3s ease;
        }
        .tarjeta-consumidor {
            background: #f9f9f9; padding: 15px; border-radius: 8px;
            margin: 10px 0; border-left: 4px solid #667eea;
        }
        .nombre-consumidor { font-weight: bold; color: #333; margin-bottom: 8px; }
        .estadisticas-consumidor {
            display: flex; justify-content: space-between;
            font-size: 13px; color: #666;
        }
        .contenedor-grafico { 
            background: white; padding: 25px; border-radius: 12px; 
            margin: 20px 0; box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        }
        .contenedor-grafico h3 {
            margin-top: 0; color: #333;
            border-bottom: 2px solid #667eea; padding-bottom: 10px;
        }
        canvas { max-height: 350px; }
        .flujo-resultados { max-height: 400px; overflow-y: auto; font-size: 13px; }
        .flujo-resultados::-webkit-scrollbar { width: 8px; }
        .flujo-resultados::-webkit-scrollbar-track { background: #f1f1f1; border-radius: 4px; }
        .flujo-resultados::-webkit-scrollbar-thumb { background: #667eea; border-radius: 4px; }
        .elemento-resultado {
            padding: 12px; border-bottom: 1px solid #eee;
            animation: fundirEntrada 0.3s;
        }
        @keyframes fundirEntrada { from { opacity: 0; } to { opacity: 1; } }
        .elemento-resultado:hover { background: #f9f9f9; }
        .marca-tiempo { color: #999; font-size: 11px; margin-top: 5px; }
        .error { color: #f44336; }
        .exito { color: #4CAF50; }
        .insignia {
            display: inline-block; padding: 3px 8px; border-radius: 10px;
            font-size: 11px; font-weight: 600; margin: 2px;
            background: #667eea; color: white;
        }
        .insignia-politica {
            display: inline-block; padding: 8px 12px; border-radius: 8px;
            font-size: 12px; font-weight: 600; margin: 5px;
            background: #4CAF50; color: white; border-left: 4px solid #2E7D32;
        }
        .estado-vacio {
            text-align: center; color: #999; padding: 40px; font-style: italic;
        }
        .estado-modelo { display: flex; align-items: center; margin: 10px 0; }
        .indicador-estado {
            width: 12px; height: 12px; border-radius: 50%; margin-right: 10px;
        }
        .estado-activo { background: #4CAF50; }
        .estado-inactivo { background: #f44336; }
        .estado-desconocido { background: #FF9800; }
        .boton-reinicio {
            background: #ff9800; color: white; border: none;
            padding: 8px 16px; border-radius: 5px; cursor: pointer;
            font-size: 12px; margin: 5px;
        }
        .boton-reinicio:hover { background: #f57c00; }
    </style>
</head>
<body>
    <div class="contenedor">
        <h2>Dashboard Universal Monte Carlo</h2>
        <div class="subtitulo">Analisis en tiempo real para cualquier modelo de simulacion</div>

        <div id="bannerEstado" class="banner-estado">Simulacion en progreso...</div>

        <div class="tarjeta">
            <h3>Politicas de Distribucion del Modelo</h3>
            <div id="politicasModelo">
                <div class="estado-vacio">Verificando estado del modelo...</div>
            </div>
            <div style="margin-top: 15px; text-align: center;">
                <button onclick="reiniciarMetricas()" class="boton-reinicio">Reiniciar Metricas Manualmente</button>
            </div>
        </div>

        <div class="cuadricula">
            <div class="tarjeta">
                <div class="etiqueta-metrica">Escenarios Procesados</div>
                <div class="valor-metrica" id="totalProcesados">0</div>
            </div>
            <div class="tarjeta">
                <div class="etiqueta-metrica">Consumidores Activos</div>
                <div class="valor-metrica" id="consumidoresActivos">0</div>
            </div>
            <div class="tarjeta">
                <div class="etiqueta-metrica">Tasa de Exito</div>
                <div class="valor-metrica" id="tasaExito">100%</div>
            </div>
            <div class="tarjeta">
                <div class="etiqueta-metrica">Errores Totales</div>
                <div class="valor-metrica error" id="totalErrores">0</div>
            </div>
        </div>

        <div class="tarjeta">
            <h3>Metricas Descubiertas del Modelo</h3>
            <div id="metricasDescubiertas" class="estado-vacio">
                Esperando resultados para descubrir metricas...
            </div>
        </div>

        <div class="contenedor-grafico">
            <h3>Valores Numericos en Tiempo Real (Stream)</h3>
            <canvas id="graficoStream"></canvas>
        </div>

        <div class="tarjeta">
            <h3>Rendimiento por Consumidor</h3>
            <div id="rendimientoConsumidor" class="estado-vacio">Esperando consumidores...</div>
        </div>

        <div class="contenedor-grafico">
            <h3>Distribucion de Carga de Trabajo</h3>
            <canvas id="graficoCarga"></canvas>
        </div>

        <div class="tarjeta">
            <h3>Stream de Resultados Recientes</h3>
            <div class="flujo-resultados" id="flujoResultados"></div>
        </div>
    </div>

    <script>
        const socket = io();
        let metricas = {};
        const colores = ['#667eea', '#764ba2', '#f093fb', '#4facfe', '#43e97b', '#fa709a', '#fee140', '#30cfd0'];
        const mapaColores = {};

        const graficoStream = new Chart(document.getElementById('graficoStream').getContext('2d'), {
            type: 'line',
            data: { labels: [], datasets: [] },
            options: { 
                animation: false, responsive: true, maintainAspectRatio: false,
                plugins: { legend: { display: true, position: 'top' } },
                scales: {
                    x: { display: true, title: { display: true, text: 'Tiempo' }, ticks: { maxRotation: 45, minRotation: 45 } },
                    y: { display: true, title: { display: true, text: 'Valor' } }
                }
            }
        });

        const graficoCarga = new Chart(document.getElementById('graficoCarga').getContext('2d'), {
            type: 'doughnut',
            data: { labels: [], datasets: [{ data: [], backgroundColor: [] }] },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right' } } }
        });

        function obtenerColor(nombre) {
            if (!mapaColores[nombre]) {
                mapaColores[nombre] = colores[Object.keys(mapaColores).length % colores.length];
            }
            return mapaColores[nombre];
        }

        function actualizarPoliticasModelo(info) {
            if (!info) return;
            const div = document.getElementById('politicasModelo');
            const claseEstado = info.estado_cola === 'Activa' ? 'estado-activo' : 
                               info.estado_cola === 'No existe' ? 'estado-inactivo' : 'estado-desconocido';
            const ultimaAct = info.ultima_actualizacion ? 
                new Date(info.ultima_actualizacion * 1000).toLocaleTimeString() : 'Nunca';
            
            div.innerHTML = `
                <div class="estado-modelo">
                    <div class="indicador-estado ${claseEstado}"></div>
                    <div><strong>Estado de la cola:</strong> ${info.estado_cola}
                    ${info.cantidad_mensajes > 0 ? '(' + info.cantidad_mensajes + ' mensaje(s))' : ''}</div>
                </div>
                <div style="margin: 15px 0;">
                    <span class="insignia-politica">Cola especifica: cola_modelo</span>
                    <span class="insignia-politica">Time-out delivery: ${info.ttl_segundos} segundos</span>
                    <span class="insignia-politica">${info.politica}</span>
                    ${info.version !== 'Desconocida' ? '<span class="insignia-politica">Version: ' + info.version + '</span>' : ''}
                    ${info.archivo_actual !== 'Desconocido' ? '<span class="insignia-politica">Archivo: ' + info.archivo_actual + '</span>' : ''}
                </div>
                <div style="color: #666; font-size: 12px; margin-top: 10px;">
                    <strong>Ultima verificacion:</strong> ${ultimaAct}
                </div>`;
        }

        function extraerNumericos(obj, prefijo = '') {
            const nums = {};
            for (const [clave, valor] of Object.entries(obj)) {
                const claveCompleta = prefijo ? prefijo + '.' + clave : clave;
                if (typeof valor === 'number') {
                    nums[claveCompleta] = valor;
                } else if (typeof valor === 'object' && valor !== null && !Array.isArray(valor)) {
                    Object.assign(nums, extraerNumericos(valor, claveCompleta));
                }
            }
            return nums;
        }

        function actualizarGraficoStream(resultado, marca) {
            const nums = extraerNumericos(resultado);
            Object.entries(nums).forEach(([nombre, valor]) => {
                let ds = graficoStream.data.datasets.find(d => d.label === nombre);
                if (!ds) {
                    ds = {
                        label: nombre, data: [], borderColor: obtenerColor(nombre),
                        backgroundColor: obtenerColor(nombre) + '20',
                        tension: 0.2, fill: false, pointRadius: 2, borderWidth: 2
                    };
                    graficoStream.data.datasets.push(ds);
                }
                ds.data.push(valor);
                if (ds.data.length > 50) ds.data.shift();
            });
            graficoStream.data.labels.push(marca);
            if (graficoStream.data.labels.length > 50) graficoStream.data.labels.shift();
            graficoStream.update('quiet');
        }

        function actualizarMetricas(datos) {
            metricas = datos;
            document.getElementById('totalProcesados').textContent = datos.total_procesados || 0;
            document.getElementById('consumidoresActivos').textContent = datos.consumidores_activos || 0;
            document.getElementById('totalErrores').textContent = datos.total_errores || 0;
            const tasa = datos.total_procesados > 0 
                ? ((datos.total_procesados - datos.total_errores) / datos.total_procesados * 100).toFixed(1) : 100;
            document.getElementById('tasaExito').textContent = tasa + '%';
            if (datos.info_modelo) actualizarPoliticasModelo(datos.info_modelo);
            actualizarMetricasDescubiertas(datos.metricas_descubiertas, datos.tipos_metricas);
            actualizarRendimiento(datos.rendimiento_consumidor);
            actualizarCarga(datos.carga_trabajo_consumidor);
        }

        function actualizarMetricasDescubiertas(descubiertas, tipos) {
            if (!descubiertas || descubiertas.length === 0) return;
            let html = '<div style="display: flex; flex-wrap: wrap; gap: 8px;">';
            descubiertas.forEach(m => {
                const tipo = tipos[m] || 'desconocido';
                const icono = tipo === 'numerica' ? '[N]' : '[T]';
                html += '<span class="insignia">' + icono + ' ' + m + ' (' + tipo + ')</span>';
            });
            document.getElementById('metricasDescubiertas').innerHTML = html + '</div>';
        }

        function actualizarRendimiento(rend) {
            if (!rend || Object.keys(rend).length === 0) return;
            let html = '';
            for (const [cons, stats] of Object.entries(rend)) {
                html += `
                    <div class="tarjeta-consumidor">
                        <div class="nombre-consumidor">[C] ${cons}</div>
                        <div class="barra-progreso">
                            <div class="relleno-progreso" style="width: ${stats.tasa_exito}%"></div>
                        </div>
                        <div class="estadisticas-consumidor">
                            <span>Procesados: <strong>${stats.procesados}</strong></span>
                            <span>Errores: <strong class="error">${stats.errores}</strong></span>
                            <span>Exito: <strong class="exito">${stats.tasa_exito.toFixed(1)}%</strong></span>
                        </div>
                    </div>`;
            }
            document.getElementById('rendimientoConsumidor').innerHTML = html;
        }

        function actualizarCarga(carga) {
            if (!carga || Object.keys(carga).length === 0) return;
            const etiquetas = Object.keys(carga);
            graficoCarga.data.labels = etiquetas;
            graficoCarga.data.datasets[0].data = Object.values(carga);
            graficoCarga.data.datasets[0].backgroundColor = etiquetas.map((_, i) => colores[i % colores.length]);
            graficoCarga.update('none');
        }

        function reiniciarMetricas() {
            if (confirm('Seguro que quieres reiniciar todas las metricas? Esto borrara todos los datos actuales.')) {
                socket.emit('reiniciar_metricas');
                actualizarMetricas({
                    total_procesados: 0, total_errores: 0, consumidores_activos: 0,
                    metricas_descubiertas: [], tipos_metricas: {},
                    rendimiento_consumidor: {}, carga_trabajo_consumidor: {},
                    info_modelo: metricas.info_modelo || {
                        estado_cola: "Desconocido", cantidad_mensajes: 0, ttl_segundos: 300,
                        politica: "Caducidad al cargar nuevo modelo",
                        version: "Desconocida", archivo_actual: "Desconocido"
                    }
                });
                graficoStream.data.labels = [];
                graficoStream.data.datasets = [];
                graficoStream.update();
                graficoCarga.data.labels = [];
                graficoCarga.data.datasets[0].data = [];
                graficoCarga.update();
                document.getElementById('flujoResultados').innerHTML = '';
                alert('Metricas reiniciadas. Esperando nuevos datos...');
            }
        }

        socket.on('modelo_actualizado', (datos) => {
            console.log("Modelo actualizado recibido:", datos);
            const banner = document.getElementById('bannerEstado');
            banner.innerHTML = 'Modelo Actualizado: ' + datos.archivo_modelo + ' | Version: ' + datos.nueva_version;
            banner.style.background = 'linear-gradient(135deg, #FF9800, #F57C00)';
            banner.classList.add('mostrar', 'cambio-modelo');
            reiniciarMetricas();
            setTimeout(() => banner.classList.remove('mostrar', 'cambio-modelo'), 5000);
        });

        socket.on('resultado', (r) => {
            if (!document.getElementById('bannerEstado').classList.contains('mostrar')) {
                document.getElementById('bannerEstado').classList.add('mostrar');
            }
            const marca = new Date(r.marca_tiempo * 1000).toLocaleTimeString();
            actualizarGraficoStream(r.resultado || {}, marca);
            const icono = r.exito ? '[OK]' : '[ERR]';
            const clase = r.exito ? 'exito' : 'error';
            const div = document.createElement('div');
            div.className = 'elemento-resultado';
            const str = JSON.stringify(r.resultado, null, 2).replace(/[{}]/g, '').replace(/"/g, '').replace(/,/g, ' |');
            div.innerHTML = `
                <div><span class="${clase}">${icono}</span> <strong>${r.consumidor}</strong></div>
                <div style="margin-left: 24px; color: #666; font-size: 12px;">${str}</div>
                <div class="marca-tiempo">${marca} | ${(r.tiempo_procesamiento * 1000).toFixed(2)}ms</div>`;
            const flujo = document.getElementById('flujoResultados');
            flujo.insertBefore(div, flujo.firstChild);
            if (flujo.children.length > 50) flujo.removeChild(flujo.lastChild);
        });

        socket.on('actualizacion_metricas', actualizarMetricas);

        socket.on('simulacion_terminada', (datos) => {
            const banner = document.getElementById('bannerEstado');
            banner.textContent = 'Simulacion Completada: ' + datos.total_procesados + ' escenarios procesados';
            banner.classList.add('terminado');
            actualizarMetricas(datos);
        });

        socket.on('estadisticas', (s) => console.log("Estadisticas consumidor:", s));
    </script>
</body>
</html>
"""

    def __init__(self, host='0.0.0.0', puerto=5000, host_rabbit='10.163.238.60', 
                 puerto_rabbit=5672, usuario_rabbit='admin', contrasena_rabbit='admin'):
        self.host = host
        self.puerto = puerto
        self.app = Flask(__name__)
        self.socketio = SocketIO(self.app, cors_allowed_origins="*", async_mode='eventlet')
        self.metricas = MetricasDashboard()
        self.oyente_rabbit = OyenteRabbitMonteCarlo(
            host=host_rabbit, port=puerto_rabbit,
            usuario=usuario_rabbit, contrasena=contrasena_rabbit,
            socketio=self.socketio, metricas=self.metricas
        )
        self._configurar_rutas()

    def _configurar_rutas(self):
        @self.app.route('/')
        def index():
            return render_template_string(self.TEMPLATE)

        @self.app.route('/metricas')
        def obtener_metricas():
            return self.metricas.obtener_resumen()

        @self.app.route('/reiniciar', methods=['POST'])
        def reiniciar():
            self.metricas.reiniciar_metricas()
            return {"status": "success", "message": "Metricas reiniciadas"}

    def ejecutar(self, depurar=True):
        threading.Thread(target=self.oyente_rabbit.ejecutar, daemon=True).start()
        print(f"[DASHBOARD] Iniciando Dashboard Universal")
        print(f"[DASHBOARD] Abre tu navegador en: http://{self.host}:{self.puerto}")
        print(f"[DASHBOARD] Conectando a RabbitMQ: {self.oyente_rabbit.host}:{self.oyente_rabbit.puerto}")
        print(f"[DASHBOARD] Usuario: {self.oyente_rabbit.usuario}")
        
        try:
            self.socketio.run(self.app, host=self.host, port=self.puerto, 
                            debug=depurar, use_reloader=False)
        except KeyboardInterrupt:
            print("\n[DASHBOARD] Cerrando dashboard...")
            self.oyente_rabbit.ejecutando = False


def main():
    dashboard = DashboardMonteCarlo(
        host='0.0.0.0',
        puerto=5000,
        host_rabbit='10.163.238.60',
        puerto_rabbit=5672,
        usuario_rabbit='admin',
        contrasena_rabbit='admin'
    )
    
    print("\nDASHBOARD INICIADO")
    print(f"Servidor RabbitMQ: 10.163.238.60:5672")
    print(f"Usuario: admin")
    print(f"Dashboard: http://0.0.0.0:5000")
    
    dashboard.ejecutar(depurar=True)


if __name__ == '__main__':
    main()