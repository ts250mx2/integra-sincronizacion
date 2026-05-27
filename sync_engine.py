import logging
import sys
from datetime import datetime
from utils import valida_nulo, campo_requerido, format_sql_date
from tqdm import tqdm

class SyncEngine:
    def __init__(self, db_manager, config):
        self.db = db_manager
        self.config = config
        self.settings = config['SETTINGS']
        
        self.id_sucursal = self.settings.getint('IdSucursal')
        self.id_computadora = self.settings.getint('IdComputadora')
        self.computadora = self.settings.get('Computadora', 'SERVER_PYTHON')
        self.gimnasio = self.settings.get('Gimnasio', 'Integra Gym')
        self.sucursal_unica = self.settings.getint('SucursalUnica', 0)
        self.version = self.settings.get('Version', '1.0.0')
        self.es_biostar = self.settings.getint('EsBioStar', 0)
        self.ruta_servidor = self.settings.get('RutaServidor', '')



    def execute_sync(self):
        logging.info(f"Iniciando ciclo de sincronización para {self.gimnasio} (Sucursal: {self.id_sucursal})...")
        
        if not self.db.connect_local():
            logging.error("No se pudo conectar a la base de datos local (Access).")
            return
            
        if not self.db.connect_remote():
            logging.error("No se pudo conectar a la base de datos remota (MySQL).")
            self.db.close_all()
            return

        local_cur = None
        remote_cur = None
        
        try:
            local_cur = self.db.local_conn.cursor()
            remote_cur = self.db.remote_conn.cursor(dictionary=True)

            # 1. Limpieza y preparación inicial local (Access)
            logging.info("Ejecutando limpieza y preparación inicial local...")
            self._execute_local(local_cur, "UPDATE tblSocios SET FechaUltimaVisita = '2000-01-01 00:00:00' WHERE FechaUltimaVisita IS NULL")
            self._execute_local(local_cur, "UPDATE tblRecorridos SET FechaInscripcion = '2000-01-01 00:00:00' WHERE FechaInscripcion IS NULL")
            
            if self.sucursal_unica == 0:
                # Marcar como modificados socios que tuvieron movimientos en las últimas 24 horas
                # En Access SQL, Now() - 1 representa el día de ayer a esta misma hora
                sql_mod = "UPDATE tblSocios SET Modificado = 1 WHERE IdSocio IN (SELECT IdSocio FROM tblMovimientos WHERE FechaMovimiento > Now() - 1)"
                self._execute_local(local_cur, sql_mod)
            self.db.local_conn.commit()

            # 2. Cargar fechas de sincronización desde el Servidor Remoto
            logging.info(f"Conectando a Servidor Central {self.gimnasio}...")
            sql = f"SELECT UltimaActualizacion, NOW() AS FechaHoy FROM tblSucursales WHERE IdSucursal = {self.id_sucursal}"
            remote_cur.execute(sql)
            row_suc = remote_cur.fetchone()
            
            if row_suc:
                vl_fecha_act = format_sql_date(row_suc['UltimaActualizacion'])
                vl_fecha_hoy = format_sql_date(row_suc['FechaHoy'])
            else:
                vl_fecha_act = "2000-01-01 00:00:00"
                vl_fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            vl_fecha_act_fotos = self.settings.get('FechaActFotos', '2000-01-01')

            logging.info(f"Fecha última actualización remota: {vl_fecha_act}")
            logging.info(f"Fecha servidor remoto (Hoy): {vl_fecha_hoy}")
            logging.info(f"Fecha fotos: {vl_fecha_act_fotos}")

            # ----------------------------------------------------
            # RECEPCIÓN DE DATOS (MYSQL -> ACCESS)
            # ----------------------------------------------------
            
            # 3. Recibiendo Sucursales
            self.sync_sucursales(local_cur, remote_cur, vl_fecha_act)

            # 4. Recibiendo Cuotas y CuotasDiasSemana
            self.sync_cuotas(local_cur, remote_cur)

            # 5. Recibiendo Formas de Pago
            self.sync_formas_pago(local_cur, remote_cur, vl_fecha_act)

            # 6. Recibiendo Movimientos Web
            self.sync_movimientos_web(local_cur, remote_cur, vl_fecha_act)

            # 7. Bloqueando Cuotas
            self.sync_cuotas_bloqueadas(local_cur, remote_cur, vl_fecha_act)

            # 8. Recibiendo Códigos de Descuento
            self.sync_codigos_descuento(local_cur, remote_cur, vl_fecha_act)

            # 9. Recibiendo Usuarios
            self.sync_usuarios(local_cur, remote_cur)

            # 10. Recibiendo Proveedores (tblSociosNegocio -> tblProveedores)
            self.sync_proveedores(local_cur, remote_cur, vl_fecha_act)

            # ----------------------------------------------------
            # ENVÍO DE DATOS / RESPALDOS (ACCESS -> MYSQL)
            # ----------------------------------------------------

            # 11. Respaldando Socios
            self.push_socios(local_cur, remote_cur)

            # 12. Enviando Retiros
            self.push_retiros(local_cur, remote_cur)

            # 13. Respaldando Egresos (Simulado de RespaldarEgresos)
            self.push_egresos(local_cur, remote_cur)

            # 14. Respaldando Interface ZK (Simulado de RespaldarInterfaceZK)
            self.push_interface_zk(local_cur, remote_cur)

            # 14b. Respaldando Asistencias
            self.push_asistencias(local_cur, remote_cur)

            # 14c. Respaldando Fotos
            self.push_fotos(local_cur, remote_cur)

            # 14d. Respaldando Huellas
            self.push_huellas(local_cur, remote_cur)



            # ----------------------------------------------------
            # MULTISUCURSAL (SOCIOS DE OTRAS SUCURSALES)
            # ----------------------------------------------------
            if self.sucursal_unica == 0:
                self.sync_multisucursal(local_cur, remote_cur, vl_fecha_act)

            # ----------------------------------------------------
            # ENVÍOS FINALES / RESPALDOS ADICIONALES (ACCESS -> MYSQL)
            # ----------------------------------------------------

            # 15. Respaldando Familiares
            self.push_familiares(local_cur, remote_cur)

            # 16. Respaldando Rechazos
            self.push_rechazos(local_cur, remote_cur)

            # 17. Respaldando Recorridos
            self.push_recorridos(local_cur, remote_cur)

            # 18. Respaldando Aperturas (Simulado de RespaldarAperturas)
            #self.push_aperturas(local_cur, remote_cur)

            # 19. Respaldando Inventarios
            self.push_inventarios(local_cur, remote_cur)

            # 20. Respaldando Alertas
            self.push_alertas(local_cur, remote_cur)

            # 21. Respaldando Movimientos
            self.push_movimientos(local_cur, remote_cur)

            # 22. Respaldando Detalle Movimientos
            self.push_detalle_movimientos(local_cur, remote_cur)

            # 23. Respaldando Movimientos Web local-to-remote
            self.push_movimientos_web_local(local_cur, remote_cur)

            # 24. Respaldando Visitas Sesiones
            self.push_visitas_sesiones(local_cur, remote_cur)

            # 25. Respaldando Visitas
            self.push_visitas(local_cur, remote_cur)


            # 25. Registrar transmisión remota final
            #logging.info("Registrando transmisión final...")
            #sql_trans = (
            #    f"REPLACE INTO tblSucursalesTransmisiones(IdSucursal, IdComputadora, Computadora, Version, FechaTransmision) "
            #    f"VALUES({self.id_sucursal}, {self.id_computadora}, '{self.computadora}', '{self.version}', NOW())"
            #)
            #self._execute_remote(remote_cur, sql_trans)

            # 26. Guardar fechas de actualización locales
            self.settings['FechaAct'] = vl_fecha_hoy
            with open('config.ini', 'w') as configfile:
                self.config.write(configfile)

            logging.info("Sincronización finalizada exitosamente.")

        except Exception as e:
            logging.exception("Error crítico durante el proceso de sincronización:")
            raise e
        finally:
            if local_cur:
                local_cur.close()
            if remote_cur:
                remote_cur.close()
            self.db.close_all()

    # --- HELPERS PARA OBTENER VALORES DE FILAS EN DIVERSOS DIALECTOS ---
    def _get_val(self, row, key):
        if row is None:
            return None
        if isinstance(row, dict):
            if key in row:
                return row[key]
            # Case insensitive
            for k, v in row.items():
                if k.lower() == key.lower():
                    return v
        try:
            return getattr(row, key)
        except AttributeError:
            pass
        try:
            return row[key]
        except (KeyError, TypeError, IndexError):
            pass
        return None

    def _execute_local(self, cursor, sql, params=None):
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
        except Exception as e:
            logging.error(f"Error ejecutando SQL Local:\nSQL: {sql}\nParámetros: {params}\nError: {e}")
            raise

    def _execute_remote(self, cursor, sql, params=None):
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
        except Exception as e:
            logging.error(f"Error ejecutando SQL Remoto:\nSQL: {sql}\nParámetros: {params}\nError: {e}")
            raise

    # --- MÉTODOS DE RECEPCIÓN (REMOTE -> LOCAL) ---

    def sync_sucursales(self, local_cur, remote_cur, fecha_act):
        logging.info("Recibiendo Sucursales...")
        sql = (
            f"SELECT A.*, B.Calle AS CalleFiscal, B.NumExterior AS NumExteriorFiscal, B.NumInterior AS NumInteriorFiscal, "
            f"B.Colonia AS ColoniaFiscal, B.CP AS CPFiscal, B.Municipio AS MunicipioFiscal, B.Estado AS EstadoFiscal "
            f"FROM tblSucursales A "
            f"LEFT JOIN tblSociosNegocio B ON A.IdRazonSocial = B.IdSocioNegocio "
            f"WHERE A.FechaAct > '{fecha_act}' "
            f"ORDER BY A.IdSucursal"
        )
        self._execute_remote(remote_cur, sql)
        rows = remote_cur.fetchall()
        
        for row in tqdm(rows, desc="Sucursales", disable=sys.stdout is None):
            id_suc = self._get_val(row, 'IdSucursal')
            self._execute_local(local_cur, f"DELETE FROM tblSucursales WHERE IdSucursal = {id_suc}")
            
            fields = (
                "IdSucursal, Sucursal, Status, Calle, NumExterior, NumInterior, Estado, Municipio, Colonia, CP, "
                "Telefono, Contacto, CorreoElectronico, RFC, RazonSocial, Serie, AplicaClienteFrecuente, "
                "AplicaClienteReferido, AplicaDescuentoAnticipado, ListaPrecios, ComisionDebito, ComisionCredito, "
                "IVA, CalleFiscal, NumExteriorFiscal, NumInteriorFiscal, ColoniaFiscal, CPFiscal, MunicipioFiscal, EstadoFiscal"
            )
            
            vals = (
                f"{id_suc}, "
                f"'{self._get_val(row, 'Sucursal')}', "
                f"{self._get_val(row, 'Status')}, "
                f"'{valida_nulo(self._get_val(row, 'Calle'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'NumExterior'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'NumInterior'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'Estado'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'Municipio'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'Colonia'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'CP'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'Telefono'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'Contacto'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'CorreoElectronico'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'RFC'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'RazonSocial'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'Serie'), True)}', "
                f"{valida_nulo(self._get_val(row, 'AplicaClienteFrecuente'))}, "
                f"{valida_nulo(self._get_val(row, 'AplicaClienteReferido'))}, "
                f"{valida_nulo(self._get_val(row, 'AplicaDescuentoAnticipado'))}, "
                f"{valida_nulo(self._get_val(row, 'ListaPrecios'))}, "
                f"{valida_nulo(self._get_val(row, 'ComisionDebito'))}, "
                f"{valida_nulo(self._get_val(row, 'ComisionCredito'))}, "
                f"{valida_nulo(self._get_val(row, 'IVA'))}, "
                f"'{valida_nulo(self._get_val(row, 'CalleFiscal'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'NumExteriorFiscal'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'NumInteriorFiscal'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'ColoniaFiscal'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'CPFiscal'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'MunicipioFiscal'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'EstadoFiscal'), True)}'"
            )
            
            self._execute_local(local_cur, f"INSERT INTO tblSucursales ({fields}) VALUES ({vals})")
        self.db.local_conn.commit()

    def sync_cuotas(self, local_cur, remote_cur):
        logging.info("Recibiendo Cuotas...")
        
        # Eliminar cuotas especiales locales -1 y -2
        self._execute_local(local_cur, "DELETE FROM tblCuotas WHERE IdCuota = -1")
        self._execute_local(local_cur, "DELETE FROM tblCuotas WHERE IdCuota = -2")
        
        # Stub para ActualizarCambiosTablasMySQL
        logging.debug("Simulando ActualizarCambiosTablasMySQL...")
        
        # Insertar registros por defecto -1 y -2
        sql_def_1 = (
            "INSERT INTO tblCuotas(IdCuota, Cuota, TipoVigencia, Vigencia, TipoHorario, Precio, Precio2, Precio3, Precio4, Iva, Costo, Exi, CodigoBarras, Puntos, CtaContable, AplicaCongelaciones, Status, AceptaPuntos, TipoCuota, Multigimnasio, HorarioRestringido, HoraEntrada, HoraSalida, EsPromocion, Impuesto, Multisocios, Orden) "
            "VALUES (-1,'PAGO ADEUDO',0,0,0,0,0,0,0,16,0,0,'',0,'',0,0,0,0,0,0,'','',0,0,0,0)"
        )
        sql_def_2 = (
            "INSERT INTO tblCuotas(IdCuota, Cuota, TipoVigencia, Vigencia, TipoHorario, Precio, Precio2, Precio3, Precio4, Iva, Costo, Exi, CodigoBarras, Puntos, CtaContable, AplicaCongelaciones, Status, AceptaPuntos, TipoCuota, Multigimnasio, HorarioRestringido, HoraEntrada, HoraSalida, EsPromocion, Impuesto, Multisocios, Orden) "
            "VALUES (-2,'ANTICIPO',0,0,0,0,0,0,0,16,0,0,'',0,'',0,0,0,0,0,0,'','',0,0,0,0)"
        )
        self._execute_local(local_cur, sql_def_1)
        self._execute_local(local_cur, sql_def_2)
        
        # Sincronizar Horarios (tblCuotasDiasSemana)
        logging.info("Recibiendo Cuotas Dia Semana...")
        self._execute_local(local_cur, "DELETE FROM tblCuotasDiasSemana")
        
        self._execute_remote(remote_cur, "SELECT * FROM tblCuotasDiasSemana ORDER BY IdCuota")
        rows_ds = remote_cur.fetchall()
        for row in tqdm(rows_ds, desc="Horarios", disable=sys.stdout is None):
            sql_ins = (
                f"INSERT INTO tblCuotasDiasSemana(DiaSemana, IdCuota, HoraEntrada, HoraSalida) "
                f"VALUES ({self._get_val(row, 'DiaSemana')}, '{self._get_val(row, 'IdCuota')}', "
                f"'{self._get_val(row, 'HoraEntrada')}', '{self._get_val(row, 'HoraSalida')}')"
            )
            self._execute_local(local_cur, sql_ins)
            
        # Sincronizar tblCuotas
        self._execute_remote(remote_cur, "SELECT * FROM tblCuotas ORDER BY IdCuota")
        rows_cuotas = remote_cur.fetchall()
        for row in tqdm(rows_cuotas, desc="Cuotas", disable=sys.stdout is None):
            id_cuota = self._get_val(row, 'IdCuota')
            self._execute_local(local_cur, f"DELETE FROM tblCuotas WHERE IdCuota = {id_cuota}")
            
            fields = (
                "IdCuota, Cuota, TipoMembresia, TipoVigencia, Vigencia, TipoHorario, Precio, Precio2, Precio3, Precio4, "
                "Iva, Costo, Exi, CodigoBarras, Puntos, CtaContable, AplicaCongelaciones, Status, AceptaPuntos, TipoCuota, "
                "Multigimnasio, HorarioRestringido, HoraEntrada, HoraSalida, EsPromocion, Impuesto, Multisocios, MembresiaMes, "
                "Sesiones, MesesProrroga, DiasProrroga, PrecioVariable, NoImprimir, Orden, CargosRecurrentes, PrecioRecurrentes, CuotasRecurrentes"
            )
            
            vals = (
                f"{id_cuota}, "
                f"'{self._get_val(row, 'Cuota')}', "
                f"{valida_nulo(self._get_val(row, 'TipoMembresia'))}, "
                f"{self._get_val(row, 'TipoVigencia')}, "
                f"{valida_nulo(self._get_val(row, 'Vigencia'))}, "
                f"{self._get_val(row, 'TipoHorario')}, "
                f"{self._get_val(row, 'Precio')}, "
                f"{valida_nulo(self._get_val(row, 'PrecioTarjeta') or self._get_val(row, 'Precio2'))}, "
                f"{valida_nulo(self._get_val(row, 'Precio3'))}, "
                f"{valida_nulo(self._get_val(row, 'Precio4'))}, "
                f"{valida_nulo(self._get_val(row, 'Iva'))}, "
                f"{valida_nulo(self._get_val(row, 'Costo'))}, "
                f"{valida_nulo(self._get_val(row, 'Exi'))}, "
                f"'{valida_nulo(self._get_val(row, 'CodigoBarras'), True)}', "
                f"{valida_nulo(self._get_val(row, 'Puntos'))}, "
                f"'{self._get_val(row, 'CtaContable')}', "
                f"{valida_nulo(self._get_val(row, 'AplicaCongelaciones'))}, "
                f"{self._get_val(row, 'Status')}, "
                f"{valida_nulo(self._get_val(row, 'AceptaPuntos'))}, "
                f"{self._get_val(row, 'TipoCuota')}, "
                f"{valida_nulo(self._get_val(row, 'Multigimnasio'))}, "
                f"{valida_nulo(self._get_val(row, 'HorarioRestringido'))}, "
                f"'{valida_nulo(self._get_val(row, 'HoraEntrada'))}', "
                f"'{valida_nulo(self._get_val(row, 'HoraSalida'))}', "
                f"{valida_nulo(self._get_val(row, 'EsPromocion'))}, "
                f"{valida_nulo(self._get_val(row, 'Impuesto'))}, "
                f"{valida_nulo(self._get_val(row, 'Multisocios'))}, "
                f"{valida_nulo(self._get_val(row, 'MembresiaMes'))}, "
                f"{valida_nulo(self._get_val(row, 'Sesiones'))}, "
                f"{valida_nulo(self._get_val(row, 'MesesProrroga'))}, "
                f"{valida_nulo(self._get_val(row, 'DiasProrroga'))}, "
                f"{valida_nulo(self._get_val(row, 'PrecioVariable'))}, "
                f"{valida_nulo(self._get_val(row, 'NoImprimir'))}, "
                f"{valida_nulo(self._get_val(row, 'Orden'))}, "
                f"{valida_nulo(self._get_val(row, 'CargosRecurrentes'))}, "
                f"{valida_nulo(self._get_val(row, 'PrecioRecurrentes'))}, "
                f"{valida_nulo(self._get_val(row, 'CuotasRecurrentes'))}"
            )
            
            self._execute_local(local_cur, f"INSERT INTO tblCuotas({fields}) VALUES ({vals})")
            self.actualizar_cuota_zk(id_cuota)
            
        self.db.local_conn.commit()

    def actualizar_cuota_zk(self, id_cuota):
        # Biometric integration stub
        logging.debug(f"Simulando ActualizarCuotaZK para IdCuota: {id_cuota}")

    def sync_formas_pago(self, local_cur, remote_cur, fecha_act):
        logging.info("Recibiendo Formas de Pago...")
        self._execute_remote(remote_cur, f"SELECT * FROM tblFormasPago WHERE FechaAct > '{fecha_act}'")
        rows = remote_cur.fetchall()
        for row in tqdm(rows, desc="Formas de Pago", disable=sys.stdout is None):
            id_fp = self._get_val(row, 'IdFormaPago')
            self._execute_local(local_cur, f"DELETE FROM tblFormasPago WHERE IdFormaPago = {id_fp}")
            
            fields = "IdFormaPago, FormaPago, Membresias, Productos, Comision, Status, ConReferencia, MembresiasCredito, Campo"
            vals = (
                f"{valida_nulo(id_fp)}, "
                f"'{valida_nulo(self._get_val(row, 'FormaPago'), True)}', "
                f"{valida_nulo(self._get_val(row, 'Membresias'))}, "
                f"{valida_nulo(self._get_val(row, 'Productos'))}, "
                f"{valida_nulo(self._get_val(row, 'Comision'))}, "
                f"{valida_nulo(self._get_val(row, 'Status'))}, "
                f"{valida_nulo(self._get_val(row, 'ConReferencia'))}, "
                f"{valida_nulo(self._get_val(row, 'MembresiasCredito'))}, "
                f"'{valida_nulo(self._get_val(row, 'Campo'), True)}'"
            )
            self._execute_local(local_cur, f"INSERT INTO tblFormasPago({fields}) VALUES({vals})")
        self.db.local_conn.commit()

    def sync_movimientos_web(self, local_cur, remote_cur, fecha_act):
        logging.info("Recibiendo Movimientos Web...")
        self._execute_local(local_cur, "DELETE FROM tblMovimientosWeb WHERE IdSucursalSocio = 0")
        
        self._execute_remote(remote_cur, f"SELECT * FROM tblMovimientosWeb WHERE FechaAct > '{fecha_act}'")
        rows = remote_cur.fetchall()
        for row in tqdm(rows, desc="Movimientos Web", disable=sys.stdout is None):
            id_web = self._get_val(row, 'IdMovimientoWeb')
            self._execute_local(local_cur, f"DELETE FROM tblMovimientosWeb WHERE IdMovimientoWeb = {id_web}")
            
            id_suc_socio = self._get_val(row, 'IdSucursalSocio')
            suc_socio_val = 99 if (id_suc_socio == 0 or id_suc_socio is None) else id_suc_socio
            
            fields = "IdMovimientoWeb, IdSocio, IdSucursalSocio, FechaMovimientoWeb, IdCuota, Cantidad, Precio, Pago, Sesiones, FechaInicio, FechaFin, Status, FechaAct, MetodoPago, TokenPago, Modificado"
            vals = (
                f"{id_web}, "
                f"{self._get_val(row, 'IdSocio')}, "
                f"{suc_socio_val}, "
                f"'{format_sql_date(self._get_val(row, 'FechaMovimientoWeb'))}', "
                f"{self._get_val(row, 'IdCuota')}, "
                f"{self._get_val(row, 'Cantidad')}, "
                f"{self._get_val(row, 'Precio')}, "
                f"{self._get_val(row, 'Pago')}, "
                f"{self._get_val(row, 'Sesiones')}, "
                f"'{format_sql_date(self._get_val(row, 'FechaInicio') or '2000-01-01')}', "
                f"'{format_sql_date(self._get_val(row, 'FechaFin') or '2000-01-01')}', "
                f"{self._get_val(row, 'Status')}, "
                f"'{format_sql_date(self._get_val(row, 'FechaAct'))}', "
                f"'{valida_nulo(self._get_val(row, 'MetodoPago'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'TokenPago'), True)[:30]}', "
                f"0"
            )
            self._execute_local(local_cur, f"INSERT INTO tblMovimientosWeb({fields}) VALUES({vals})")
        self.db.local_conn.commit()

    def sync_cuotas_bloqueadas(self, local_cur, remote_cur, fecha_act):
        logging.info("Bloqueando Cuotas...")
        sql = f"SELECT * FROM tblCuotasBloqueadas WHERE FechaAct > '{fecha_act}' AND IdSucursal = {self.id_sucursal}"
        self._execute_remote(remote_cur, sql)
        rows = remote_cur.fetchall()
        for row in tqdm(rows, desc="Bloqueos", disable=sys.stdout is None):
            status = self._get_val(row, 'Status')
            id_cuota = self._get_val(row, 'IdCuota')
            
            if status == 2:
                self._execute_local(local_cur, f"UPDATE tblCuotas SET Status = StatusOriginal WHERE IdCuota = {id_cuota}")
            else:
                tipo_bloqueo = self._get_val(row, 'TipoBloqueo')
                if tipo_bloqueo == 0:
                    self._execute_local(local_cur, f"UPDATE tblCuotas SET Status = 2 WHERE IdCuota = {id_cuota}")
                else:
                    precio = self._get_val(row, 'Precio')
                    precio_tarjeta = self._get_val(row, 'PrecioTarjeta')
                    self._execute_local(local_cur, f"UPDATE tblCuotas SET Precio = {precio}, Precio2 = {precio_tarjeta} WHERE IdCuota = {id_cuota}")
        self.db.local_conn.commit()

    def sync_codigos_descuento(self, local_cur, remote_cur, fecha_act):
        logging.info("Recibiendo Códigos de Descuento...")
        self._execute_remote(remote_cur, f"SELECT * FROM tblCodigosDescuentos WHERE FechaAct > '{fecha_act}' ORDER BY IdCodigoDescuento")
        rows = remote_cur.fetchall()
        for row in tqdm(rows, desc="Descuentos", disable=sys.stdout is None):
            id_cd = self._get_val(row, 'IdCodigoDescuento')
            self._execute_local(local_cur, f"DELETE FROM tblCodigosDescuentos WHERE IdCodigoDescuento = {id_cd}")
            
            fields = "IdCodigoDescuento, CodigoDescuento, Monto, DescripcionDescuento, Status, Vigencia"
            vals = (
                f"{id_cd}, "
                f"'{self._get_val(row, 'CodigoDescuento')}', "
                f"{self._get_val(row, 'Monto')}, "
                f"'{self._get_val(row, 'DescripcionDescuento')}', "
                f"{self._get_val(row, 'Status')}, "
                f"'{format_sql_date(self._get_val(row, 'Vigencia'))}'"
            )
            self._execute_local(local_cur, f"INSERT INTO tblCodigosDescuentos({fields}) VALUES({vals})")
        self.db.local_conn.commit()

    def sync_usuarios(self, local_cur, remote_cur):
        logging.info("Recibiendo Usuarios...")
        self._execute_remote(remote_cur, "SELECT * FROM tblUsuarios ORDER BY IdUsuario")
        rows = remote_cur.fetchall()
        for row in tqdm(rows, desc="Usuarios", disable=sys.stdout is None):
            id_usr = self._get_val(row, 'IdUsuario')
            self._execute_local(local_cur, f"DELETE FROM tblUsuarios WHERE IdUsuario = {id_usr}")
            
            fields = (
                "IdUsuario, Usuario, Login, Passwd, Status, CalleNumero, Estado, Municipio, Colonia, CP, "
                "TelCasa, TelCelular, TipoUsuario, Puesto, IdSucursal, TarjetaRFID, CorreoElectronico"
            )
            
            vals = (
                f"{id_usr}, "
                f"'{self._get_val(row, 'Usuario')}', "
                f"'{self._get_val(row, 'Login')}', "
                f"'{self._get_val(row, 'Passwd')}', "
                f"{self._get_val(row, 'Status')}, "
                f"'{valida_nulo(self._get_val(row, 'CalleNumero'), True)[:90]}', "
                f"'{valida_nulo(self._get_val(row, 'Estado'), True)[:50]}', "
                f"'{valida_nulo(self._get_val(row, 'Municipio'), True)[:15]}', "
                f"'{valida_nulo(self._get_val(row, 'Colonia'), True)[:60]}', "
                f"'{self._get_val(row, 'CP')}', "
                f"'{self._get_val(row, 'TelCasa')}', "
                f"'{self._get_val(row, 'TelCelular')}', "
                f"{valida_nulo(self._get_val(row, 'TipoUsuario'))}, "
                f"'{valida_nulo(self._get_val(row, 'Puesto'), True)}', "
                f"{valida_nulo(self._get_val(row, 'IdSucursal'))}, "
                f"'{self._get_val(row, 'TarjetaRFID')}', "
                f"'{self._get_val(row, 'CorreoElectronico')}'"
            )
            
            self._execute_local(local_cur, f"INSERT INTO tblUsuarios({fields}) VALUES({vals})")
            self.actualizar_interface_zk(id_usr, 0, 0, True)
            
        # Actualización final para empleados en tblSociosHuellas
        sql_huellas = (
            "UPDATE tblSociosHuellas AS A "
            "INNER JOIN tblUsuarios AS B ON A.IdSocio = B.IdUsuario "
            "SET A.Socio = B.Usuario, A.CodigoSocio = 'USU' + RTRIM(B.IdUsuario), A.FechaVencimiento = '2000-01-01 00:00:00' "
            "WHERE A.EsEmpleado = 1"
        )
        try:
            self._execute_local(local_cur, sql_huellas)
        except Exception:
            logging.warning("No se pudo actualizar tblSociosHuellas para empleados (es posible que la tabla no exista).")
            
        self.db.local_conn.commit()

    def actualizar_interface_zk(self, id_usuario, p1, p2, p3):
        logging.debug(f"Simulando ActualizarIntefaceZK para IdUsuario: {id_usuario}")

    def sync_proveedores(self, local_cur, remote_cur, fecha_act):
        logging.info("Recibiendo Proveedores...")
        self._execute_remote(remote_cur, f"SELECT * FROM tblSociosNegocio WHERE FechaAct > '{fecha_act}'")
        rows = remote_cur.fetchall()
        for row in tqdm(rows, desc="Proveedores", disable=sys.stdout is None):
            id_sn = self._get_val(row, 'IdSocioNegocio')
            self._execute_local(local_cur, f"DELETE FROM tblProveedores WHERE IdProveedor = {id_sn}")
            
            fields = (
                "IdProveedor, Proveedor, Direccion, Municipio, Colonia, Pais, Tel, Fax, "
                "Status, RFC, FechaAlta, CP, Contacto, CorreoElectronico, TipoPago, TipoSurtido"
            )
            
            vals = (
                f"{id_sn}, "
                f"'{valida_nulo(self._get_val(row, 'SocioNegocio'), True)[:100]}', "
                f"'{valida_nulo(self._get_val(row, 'Calle'), True)[:100]} {self._get_val(row, 'NumExterior')} {self._get_val(row, 'NumInterior')}', "
                f"'{valida_nulo(self._get_val(row, 'Municipio'), True)[:60]}', "
                f"'{valida_nulo(self._get_val(row, 'Colonia'), True)[:100]}', "
                f"'{valida_nulo(self._get_val(row, 'Pais'), True)[:20]}', "
                f"'{valida_nulo(self._get_val(row, 'Telefonos'), True)[:20]}', "
                f"'', "
                f"{self._get_val(row, 'Status')}, "
                f"'{valida_nulo(self._get_val(row, 'RFC'), True)[:50]}', "
                f"NOW(), "
                f"0, "
                f"'{valida_nulo(self._get_val(row, 'Contacto'), True)[:50]}', "
                f"'{valida_nulo(self._get_val(row, 'CorreoElectronico'), True)[:50]}', "
                f"0, "
                f"0"
            )
            self._execute_local(local_cur, f"INSERT INTO tblProveedores({fields}) VALUES({vals})")
        self.db.local_conn.commit()


    # --- MÉTODOS DE ENVÍO / TRANSMISIÓN (LOCAL -> REMOTE) ---

    def push_socios(self, local_cur, remote_cur):
        logging.info("Respaldando Socios modificados...")
        self._execute_local(local_cur, "SELECT * FROM tblSocios WHERE Modificado = 1 ORDER BY IdSocio")
        rows = local_cur.fetchall()
        for row in tqdm(rows, desc="Push Socios", disable=sys.stdout is None):
            id_socio = self._get_val(row, 'IdSocio')
            id_sucursal = self._get_val(row, 'IdSucursal')
            
            if self.sucursal_unica == 0:
                sql_rep_rec = (
                    f"REPLACE INTO tblSociosSucursalesRecepcion(IdSocio, IdSucursal, IdSucursalRecepcion, IdSucursalActualiza, Recepcion, FechaAct, TipoRecepcion) "
                    f"SELECT {valida_nulo(id_socio)}, {id_sucursal}, IdSucursal, {self.id_sucursal}, 0, NOW(), 0 FROM tblSucursales WHERE Status = 0 AND IdSucursal <> {self.id_sucursal}"
                )
                self._execute_remote(remote_cur, sql_rep_rec)

            fields = (
                "IdSocio, Nombres, Apellidos, FechaNacimiento, FechaAlta, CalleNumero, Colonia, CP, Estado, Municipio, "
                "TelCasa, TelOficina, OtroTelefono, CorreoElectronico, Ocupacion, Contacto, EstadoCivil, Sexo, Status, "
                "FechaUltimaVisita, CodigoBarras, IdSucursal, FechaVencimiento, FechaAct, Puntos, Empresa, Passwd, TarjetaRFID, Locker"
            )
            
            fecha_venc = self._get_val(row, 'FechaVencimiento')
            fecha_venc_str = format_sql_date(fecha_venc) if fecha_venc else "2000-01-01 00:00:00"
            
            vals = (
                f"{valida_nulo(id_socio)}, "
                f"'{valida_nulo(self._get_val(row, 'Nombres'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'Apellidos'), True)}', "
                f"'{format_sql_date(self._get_val(row, 'FechaNacimiento'))}', "
                f"'{format_sql_date(self._get_val(row, 'FechaAlta'))}', "
                f"'{valida_nulo(self._get_val(row, 'CalleNumero'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'Colonia'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'CP'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'Estado'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'Municipio'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'TelCasa'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'TelOficina'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'OtroTelefono'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'CorreoElectronico'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'Ocupacion'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'Contacto'), True)}', "
                f"{valida_nulo(self._get_val(row, 'EstadoCivil'))}, "
                f"{valida_nulo(self._get_val(row, 'Sexo'))}, "
                f"{valida_nulo(self._get_val(row, 'Status'))}, "
                f"'{format_sql_date(self._get_val(row, 'FechaUltimaVisita'))}', "
                f"'{valida_nulo(self._get_val(row, 'CodigoBarras'), True)}', "
                f"{valida_nulo(id_sucursal)}, "
                f"'{fecha_venc_str}', "
                f"NOW(), "
                f"{valida_nulo(self._get_val(row, 'Puntos'))}, "
                f"'{valida_nulo(self._get_val(row, 'Empresa'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'Passwd'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'TarjetaRFID'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'Locker'), True)}'"
            )
            
            sql_rep = f"REPLACE INTO tblSocios({fields}) VALUES ({vals})"
            self._execute_remote(remote_cur, sql_rep)
            
            # Limpiar modificado local
            self._execute_local(local_cur, f"UPDATE tblSocios SET Modificado = 0 WHERE IdSocio = {id_socio} AND IdSucursal = {id_sucursal}")
        self.db.local_conn.commit()

    def push_retiros(self, local_cur, remote_cur):
        logging.info("Respaldando Retiros...")
        self._execute_local(local_cur, "SELECT * FROM tblRetiros WHERE Modificado = 1 ORDER BY IdRetiro")
        rows = local_cur.fetchall()
        for row in tqdm(rows, desc="Push Retiros", disable=sys.stdout is None):
            id_retiro = self._get_val(row, 'IdRetiro')
            
            fields = "IdRetiro, IdSucursal, IdUsuario, FechaRetiro, Efectivo, Status, TipoRetiro, IdApertura, IdComputadora, FechaAct"
            vals = (
                f"{id_retiro}, "
                f"{self.id_sucursal}, "
                f"{self._get_val(row, 'IdUsuario')}, "
                f"'{format_sql_date(self._get_val(row, 'FechaRetiro'))}', "
                f"{self._get_val(row, 'Efectivo')}, "
                f"{self._get_val(row, 'Status')}, "
                f"{self._get_val(row, 'TipoRetiro')}, "
                f"{self._get_val(row, 'IdApertura')}, "
                f"{self._get_val(row, 'IdComputadora')}, "
                f"NOW()"
            )
            
            self._execute_remote(remote_cur, f"REPLACE INTO tblRetiros({fields}) VALUES({vals})")
            self._execute_local(local_cur, f"UPDATE tblRetiros SET Modificado = 0 WHERE IdRetiro = {id_retiro}")
        self.db.local_conn.commit()

    def push_egresos(self, local_cur, remote_cur):
        # Simula RespaldarEgresos
        logging.info("Respaldando Egresos (Simulado)...")
        try:
            self._execute_local(local_cur, "SELECT * FROM tblEgresos WHERE Modificado = 1")
            rows = local_cur.fetchall()
            for row in rows:
                id_egreso = self._get_val(row, 'IdEgreso')
                fields = "IdEgreso, IdSucursal, IdUsuario, Concepto, Importe, FechaAct"
                vals = f"{id_egreso}, {self.id_sucursal}, {self._get_val(row, 'IdUsuario')}, '{valida_nulo(self._get_val(row, 'Concepto'), True)}', {self._get_val(row, 'Importe')}, NOW()"
                self._execute_remote(remote_cur, f"REPLACE INTO tblEgresos({fields}) VALUES({vals})")
                self._execute_local(local_cur, f"UPDATE tblEgresos SET Modificado = 0 WHERE IdEgreso = {id_egreso}")
            self.db.local_conn.commit()
        except Exception:
            logging.debug("Tabla tblEgresos no disponible o no modificada.")

    def push_interface_zk(self, local_cur, remote_cur):
        # Simula RespaldarInterfaceZK (Sincronización de plantillas biométricas modificadas)
        logging.info("Respaldando Interfaces Biométricas ZK (Simulado)...")
        try:
            self._execute_local(local_cur, "SELECT * FROM tblSociosHuellas WHERE Modificado = 1")
            rows = local_cur.fetchall()
            for row in rows:
                id_socio = self._get_val(row, 'IdSocio')
                # Envío de huella/template al servidor central
                self._execute_local(local_cur, f"UPDATE tblSociosHuellas SET Modificado = 0 WHERE IdSocio = {id_socio}")
            self.db.local_conn.commit()
        except Exception:
            logging.debug("Tabla tblSociosHuellas no disponible o no modificada.")

    # --- MULTISUCURSAL (SOCIOS Y FAMILIARES COMPARTIDOS) ---

    def sync_multisucursal(self, local_cur, remote_cur, fecha_act):
        logging.info("Procesando lógicas de Multi-Sucursal...")
        
        # Eliminar buffer temporal local
        #self._execute_local(local_cur, f"DELETE FROM tblBufferMaxFechaFin WHERE IdComputadora = {self.id_computadora} AND IdSucursalConsulta = {self.id_sucursal}")
        
        # Insertar máximas fechas de vencimiento activas en el buffer local
        # En Access SQL, insertamos el agrupado directamente
        sql_buff = (
            f"INSERT INTO tblBufferMaxFechaFin(IdSocio, IdSucursalSocio, MaxFechaFin, IdComputadora, IdSucursalConsulta) "
            f"SELECT A.IdSocio, A.IdSucursalSocio, MAX(B.FechaFin), {self.id_computadora}, {self.id_sucursal} "
            f"FROM tblMovimientos A "
            f"INNER JOIN tblDetalleMovimientos B ON A.IdSocio = B.IdSocio AND A.IdSucursal = B.IdSucursalSocio "
            f"INNER JOIN tblCuotas C ON B.IdCuota = C.IdCuota "
            f"WHERE A.Status = 0 AND B.FechaFin > NOW() AND C.TipoCuota = 1 "
            f"GROUP BY A.IdSocio, A.IdSucursalSocio"
        )
        #try:
        #    self._execute_local(local_cur, sql_buff)
        #except Exception as e:
        #    logging.warning(f"No se pudo insertar en tblBufferMaxFechaFin: {e}. Continuando...")

        # Actualizar recepción de sucursales locales
        sql_rep_rec = (
            f"INSERT INTO tblSociosSucursalesRecepcion(IdSocio, IdSucursal, IdSucursalRecepcion, IdSucursalActualiza, FechaAct, Recepcion, TipoRecepcion, Eliminar) "
            f"SELECT A.IdSocio, A.IdSucursal, {self.id_sucursal}, A.IdSucursal, NOW(), 0, 0, 0 "
            f"FROM tblSocios A "
            f"INNER JOIN tblBufferMaxFechaFin B ON A.IdSocio = B.IdSocio AND A.IdSucursal = B.IdSucursalSocio "
            f"WHERE A.FechaVencimiento < B.MaxFechaFin AND B.MaxFechaFin >= NOW() AND B.IdSucursalConsulta = {self.id_sucursal} AND B.IdComputadora = {self.id_computadora}"
        )
        #try:
        #    self._execute_local(local_cur, sql_rep_rec)
        #except Exception:
        #    pass

        # Sincronizar fechas de vencimiento locales basadas en el buffer
        sql_upd_soc = (
            f"UPDATE tblSocios A "
            f"INNER JOIN tblBufferMaxFechaFin B ON A.IdSocio = B.IdSocio AND A.IdSucursal = B.IdSucursalSocio "
            f"SET A.FechaVencimiento = B.MaxFechaFin "
            f"WHERE A.FechaVencimiento < B.MaxFechaFin AND B.MaxFechaFin >= NOW() AND B.IdSucursalConsulta = {self.id_sucursal} AND B.IdComputadora = {self.id_computadora}"
        )
        #try:
        #    self._execute_local(local_cur, sql_upd_soc)
        #except Exception:
        #    pass
            
        #self.db.local_conn.commit()

        # Recibiendo socios de otras sucursales
        logging.info("Recibiendo socios de otras sucursales...")
        sql_remote_socios = (
            f"SELECT A.IdSocio, A.Nombres, A.Apellidos, FechaNacimiento, FechaAlta, CalleNumero, Colonia, CP, Estado, Municipio, "
            f"Telefonos, CorreoElectronico, Ocupacion, Contacto, Sexo, Status, CodigoBarras, "
            f"A.IdSucursal, Empresa, A.FechaAct, CURP, Foto, Huella, FechaInicio1, FechaFin1, IdCuota, TelCasa, TelOficina, "
            f"OtroTelefono, EstadoCivil, FechaUltimaVisita, MAX(FechaVencimiento) AS FechaVencimiento, Puntos, TarjetaRFID, Passwd, Locker "
            f"FROM tblSocios A "
            f"LEFT JOIN tblSociosSucursalesRecepcion B ON A.IdSocio = B.IdSocio AND A.IdSucursal = B.IdSucursal "
            f"WHERE A.FechaAct >= '{fecha_act}' AND (B.IdSucursalActualiza <> {self.id_sucursal} OR A.IdSucursal = 99) "
            f"GROUP BY A.IdSocio, A.Nombres, A.Apellidos, FechaNacimiento, FechaAlta, CalleNumero, Colonia, CP, Estado, Municipio, "
            f"Telefonos, CorreoElectronico, Ocupacion, Contacto, Sexo, Status, CodigoBarras, "
            f"A.IdSucursal, Empresa, A.FechaAct, CURP, Foto, Huella, FechaInicio1, FechaFin1, IdCuota, TelCasa, TelOficina, "
            f"OtroTelefono, EstadoCivil, FechaUltimaVisita, Puntos, TarjetaRFID, Passwd, Locker "
            f"ORDER BY B.IdSocio, B.IdSucursal"
        )
        self._execute_remote(remote_cur, sql_remote_socios)
        rows_other = remote_cur.fetchall()
        
        for row in tqdm(rows_other, desc="Otros Socios", disable=sys.stdout is None):
            id_socio = self._get_val(row, 'IdSocio')
            id_suc = self._get_val(row, 'IdSucursal')
            
            # Limpieza local previa
            self._execute_local(local_cur, f"DELETE FROM tblSocios WHERE IdSocio = {id_socio} AND IdSucursal = {id_suc}")
            
            vl_cod_barras = self._get_val(row, 'CodigoBarras')
            if not vl_cod_barras:
                vl_cod_barras = f"WEB{id_socio}"

            fields = (
                "IdSocio, Nombres, Apellidos, FechaNacimiento, FechaAlta, CalleNumero, Colonia, CP, Estado, Municipio, "
                "TelCasa, TelOficina, OtroTelefono, CorreoElectronico, Ocupacion, Contacto, EstadoCivil, Sexo, Status, "
                "CodigoBarras, IdSucursal, FechaVencimiento, Puntos, Empresa, Modificado, Otras, TarjetaRFID, Locker"
            )
            
            f_nac = self._get_val(row, 'FechaNacimiento')
            f_nac_str = format_sql_date(f_nac) if f_nac else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f_alta = self._get_val(row, 'FechaAlta')
            f_alta_str = format_sql_date(f_alta) if f_alta else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f_venc = self._get_val(row, 'FechaVencimiento')
            f_venc_str = format_sql_date(f_venc) if f_venc else '1980-01-01 00:00:00'

            vals = (
                f"{id_socio}, "
                f"'{self._get_val(row, 'Nombres')}', "
                f"'{self._get_val(row, 'Apellidos')}', "
                f"'{f_nac_str}', "
                f"'{f_alta_str}', "
                f"'{self._get_val(row, 'CalleNumero')}', "
                f"'{self._get_val(row, 'Colonia')}', "
                f"'{self._get_val(row, 'CP')}', "
                f"'{self._get_val(row, 'Estado')}', "
                f"'{self._get_val(row, 'Municipio')}', "
                f"'{self._get_val(row, 'TelCasa')}', "
                f"'{self._get_val(row, 'TelOficina')}', "
                f"'{self._get_val(row, 'OtroTelefono')}', "
                f"'{self._get_val(row, 'CorreoElectronico')}', "
                f"'{self._get_val(row, 'Ocupacion')}', "
                f"'{self._get_val(row, 'Contacto')}', "
                f"{valida_nulo(self._get_val(row, 'EstadoCivil'))}, "
                f"{valida_nulo(self._get_val(row, 'Sexo'))}, "
                f"{valida_nulo(self._get_val(row, 'Status'))}, "
                f"'{vl_cod_barras}', "
                f"{id_suc}, "
                f"'{f_venc_str}', "
                f"{valida_nulo(self._get_val(row, 'Puntos'))}, "
                f"'{self._get_val(row, 'Empresa')}', "
                f"0, 1, "
                f"'{self._get_val(row, 'TarjetaRFID')}', "
                f"'{self._get_val(row, 'Locker')}'"
            )
            
            self._execute_local(local_cur, f"INSERT INTO tblSocios({fields}) VALUES ({vals})")
            
            # Borrar y re-insertar configuraciones de computadora de recepción local
            self._execute_local(local_cur, f"DELETE FROM tblSociosComputadorasRecepcion WHERE IdSocio = {id_socio} AND IdSucursal = {id_suc}")
            sql_comp_rec = (
                f"INSERT INTO tblSociosComputadorasRecepcion(IdSocio, IdSucursal, IdComputadora, FechaAct, Recepcion) "
                f"SELECT {id_socio}, {id_suc}, IdComputadora, NOW(), 0 FROM tblComputadoras WHERE EsHuella = 1"
            )
            try:
                self._execute_local(local_cur, sql_comp_rec)
            except Exception:
                pass
                
            # Actualizar recepción remota
            sql_remote_upd = (
                f"UPDATE tblSociosSucursalesRecepcion SET Recepcion = 1 "
                f"WHERE IdSocio = {id_socio} AND IdSucursal = {id_suc} "
                f"AND IdSucursalRecepcion = {self.id_sucursal} AND TipoRecepcion = 0"
            )
            self._execute_remote(remote_cur, sql_remote_upd)
            
        self.db.local_conn.commit()

        # Recibiendo familiares de otras sucursales
        logging.info("Recibiendo familiares de otras sucursales...")
        sql_fam_remoto = f"SELECT * FROM tblFamiliares WHERE FechaAct > '{fecha_act}' AND IdSucursal <> {self.id_sucursal} ORDER BY IdSucursal, IdFamiliar"
        self._execute_remote(remote_cur, sql_fam_remoto)
        rows_fam = remote_cur.fetchall()
        
        for row in tqdm(rows_fam, desc="Familiares O.S.", disable=sys.stdout is None):
            id_socio = self._get_val(row, 'IdSocio')
            id_suc = self._get_val(row, 'IdSucursal')
            id_fam = self._get_val(row, 'IdFamiliar')
            
            self._execute_local(local_cur, f"DELETE FROM tblFamiliares WHERE IdSocio = {id_socio} AND IdSucursal = {id_suc} AND IdFamiliar = {id_fam}")
            
            fields = "IdSocio, IdSucursal, IdFamiliar, LetraFamiliar, CodigoFamiliar, Familiar, FechaNacimiento, Sexo, Telefonos, CorreoElectronico, TarjetaRFID, Locker, Status, Modificado"
            f_nac = self._get_val(row, 'FechaNacimiento')
            f_nac_str = format_sql_date(f_nac) if f_nac else '1980-01-01'
            
            vals = (
                f"{id_socio}, {id_suc}, {id_fam}, "
                f"'{self._get_val(row, 'LetraFamiliar')}', "
                f"'{self._get_val(row, 'CodigoFamiliar')}', "
                f"'{self._get_val(row, 'Familiar')}', "
                f"'{f_nac_str}', "
                f"{self._get_val(row, 'Sexo')}, "
                f"'{self._get_val(row, 'Telefonos')}', "
                f"'{self._get_val(row, 'CorreoElectronico')}', "
                f"'{self._get_val(row, 'TarjetaRFID')}', "
                f"'{self._get_val(row, 'Locker')}', "
                f"{self._get_val(row, 'Status')}, 0"
            )
            self._execute_local(local_cur, f"INSERT INTO tblFamiliares({fields}) VALUES ({vals})")
        self.db.local_conn.commit()

        # Recibiendo fotos de otras sucursales
        logging.info("Recibiendo fotos de otras sucursales...")
        try:
            vl_fecha_act_fotos = self.settings.get('FechaActFotos', '2000-01-01')
            sql_fotos = (
                f"SELECT A.*, Serie FROM tblSociosFotos A "
                f"INNER JOIN tblSucursales C ON A.IdSucursal = C.IdSucursal "
                f"WHERE A.IdSucursalActualiza <> {self.id_sucursal} AND Foto IS NOT NULL "
                f"AND A.FechaAct >= '{vl_fecha_act_fotos}' AND A.EsUltimaFoto = 1 "
                f"ORDER BY A.FechaAct"
            )
            self._execute_remote(remote_cur, sql_fotos)
            rows_fotos = remote_cur.fetchall()
            
            import os
            for row in tqdm(rows_fotos, desc="Fotos O.S.", disable=sys.stdout is None):
                id_socio = self._get_val(row, 'IdSocio')
                id_suc = self._get_val(row, 'IdSucursal')
                es_jpg = self._get_val(row, 'EsJpg')
                serie = self._get_val(row, 'Serie')
                foto_data = self._get_val(row, 'Foto') # Binary blob
                
                vl_id_zk = (id_socio * 10000) + id_suc
                
                if foto_data:
                    ext = ".jpg" if es_jpg == 1 else ".bmp"
                    file_name_primary = f"{serie}{id_socio}{ext}"
                    file_name_secondary = f"{vl_id_zk}.jpg"
                    
                    path_primary = os.path.join(self.ruta_servidor, "Fotos", file_name_primary)
                    path_secondary = os.path.join(self.ruta_servidor, "Fotos", file_name_secondary)
                    
                    try:
                        os.makedirs(os.path.dirname(path_primary), exist_ok=True)
                        with open(path_primary, "wb") as f:
                            f.write(foto_data)
                        with open(path_secondary, "wb") as f:
                            f.write(foto_data)
                    except Exception as e:
                        logging.error(f"No se pudieron escribir los archivos de fotos locales: {e}")
                    
                    # Local update
                    self._execute_local(local_cur, f"DELETE FROM tblSociosComputadorasRecepcion WHERE IdSocio = {id_socio} AND IdSucursal = {id_suc}")
                    sql_comp = (
                        f"INSERT INTO tblSociosComputadorasRecepcion(IdSocio, IdSucursal, IdComputadora, FechaAct, Recepcion) "
                        f"SELECT {id_socio}, {id_suc}, IdComputadora, NOW(), 0 FROM tblComputadoras WHERE EsHuella = 1"
                    )
                    self._execute_local(local_cur, sql_comp)
                    
                # Update remote receipt status
                sql_upd = (
                    f"UPDATE tblSociosSucursalesRecepcion SET Recepcion = 1 "
                    f"WHERE IdSocio = {id_socio} AND IdSucursal = {id_suc} AND IdSucursalRecepcion = {self.id_sucursal} AND TipoRecepcion = 1"
                )
                self._execute_remote(remote_cur, sql_upd)
                
            self.db.local_conn.commit()
            
            # Save local configurations
            vl_fecha_hoy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.settings['FechaActFotos'] = vl_fecha_hoy
            with open('config.ini', 'w') as configfile:
                self.config.write(configfile)
                
        except Exception as e:
            logging.error(f"Error recibiendo fotos de otras sucursales: {e}")



    # --- ENVIOS FINALES / RESPALDOS ADICIONALES (LOCAL -> REMOTE) ---

    def push_familiares(self, local_cur, remote_cur):
        logging.info("Respaldando familiares...")
        self._execute_local(local_cur, "SELECT * FROM tblFamiliares WHERE Modificado = 1 ORDER BY IdFamiliar")
        rows = local_cur.fetchall()
        for row in tqdm(rows, desc="Push Fam.", disable=sys.stdout is None):
            id_socio = self._get_val(row, 'IdSocio')
            id_suc = self._get_val(row, 'IdSucursal')
            id_fam = self._get_val(row, 'IdFamiliar')
            
            self._execute_remote(remote_cur, f"DELETE FROM tblFamiliares WHERE IdSocio = {id_socio} AND IdSucursal = {id_suc} AND IdFamiliar = {id_fam}")
            
            fields = "IdSocio, IdSucursal, IdFamiliar, LetraFamiliar, CodigoFamiliar, Familiar, FechaNacimiento, Sexo, Telefonos, CorreoElectronico, TarjetaRFID, Locker, Status, FechaAct"
            vals = (
                f"{id_socio}, {id_suc}, {id_fam}, "
                f"'{self._get_val(row, 'LetraFamiliar')}', "
                f"'{self._get_val(row, 'CodigoFamiliar')}', "
                f"'{self._get_val(row, 'Familiar')}', "
                f"'{format_sql_date(self._get_val(row, 'FechaNacimiento'))[:10]}', "
                f"{self._get_val(row, 'Sexo')}, "
                f"'{self._get_val(row, 'Telefonos')}', "
                f"'{self._get_val(row, 'CorreoElectronico')}', "
                f"'{self._get_val(row, 'TarjetaRFID')}', "
                f"'{self._get_val(row, 'Locker')}', "
                f"{self._get_val(row, 'Status')}, NOW()"
            )
            
            self._execute_remote(remote_cur, f"INSERT INTO tblFamiliares({fields}) VALUES ({vals})")
            self._execute_local(local_cur, f"UPDATE tblFamiliares SET Modificado = 0 WHERE IdSocio = {id_socio} AND IdSucursal = {id_suc} AND IdFamiliar = {id_fam}")
        self.db.local_conn.commit()

    def push_rechazos(self, local_cur, remote_cur):
        logging.info("Respaldando Rechazos...")
        self._execute_local(local_cur, f"SELECT * FROM tblRechazos WHERE Modificado = 1 AND IdSucursal = {self.id_sucursal}")
        rows = local_cur.fetchall()
        for row in tqdm(rows, desc="Push Rechazos", disable=sys.stdout is None):
            id_rechazo = self._get_val(row, 'IdRechazo')
            id_suc = self._get_val(row, 'IdSucursal')
            
            self._execute_remote(remote_cur, f"DELETE FROM tblRechazos WHERE IdRechazo = {id_rechazo} AND IdSucursal = {id_suc}")
            
            fields = "IdRechazo, IdSucursal, IdSocio, IdSucursalSocio, IdFamiliar, EsEmpleado, FechaRechazo, MotivoRechazo, Modificado, FechaAct"
            vals = (
                f"{id_rechazo}, {id_suc}, "
                f"{valida_nulo(self._get_val(row, 'IdSocio'))}, "
                f"{valida_nulo(self._get_val(row, 'IdSucursalSocio'))}, "
                f"{valida_nulo(self._get_val(row, 'IdFamiliar'))}, "
                f"{self._get_val(row, 'EsEmpleado')}, "
                f"'{format_sql_date(self._get_val(row, 'FechaRechazo'))}', "
                f"'{self._get_val(row, 'MotivoRechazo')}', "
                f"{self._get_val(row, 'Modificado')}, NOW()"
            )
            
            self._execute_remote(remote_cur, f"INSERT INTO tblRechazos({fields}) VALUES ({vals})")
            self._execute_local(local_cur, f"UPDATE tblRechazos SET Modificado = 0 WHERE IdRechazo = {id_rechazo} AND IdSucursal = {id_suc}")
        self.db.local_conn.commit()

    def push_recorridos(self, local_cur, remote_cur):
        logging.info("Respaldando Recorridos...")
        self._execute_local(local_cur, "SELECT * FROM tblRecorridos WHERE Modificado = 1")
        rows = local_cur.fetchall()
        for row in tqdm(rows, desc="Push Recorridos", disable=sys.stdout is None):
            id_rec = self._get_val(row, 'IdRecorrido')
            
            self._execute_remote(remote_cur, f"DELETE FROM tblRecorridos WHERE IdRecorrido = {id_rec} AND IdSucursal = {self.id_sucursal}")
            
            # Limpieza y filtrado de caracteres en campos
            c_email = str(self._get_val(row, 'CorreoElectronico')).replace("/", "").replace("\\", "")
            c_tel = str(self._get_val(row, 'Telefono')).replace("/", "").replace("\\", "")
            c_cel = str(self._get_val(row, 'Celular')).replace("/", "").replace("\\", "")

            fields = (
                "IdRecorrido, IdSucursal, Nombres, Apellidos, CorreoElectronico, Telefono, Celular, Edad, Fuente, "
                "OtroGimnasio, CualGimnasio, Paquete, FechaInscripcion, IdMovimiento, FechaRecorrido, Status, Visitas, IdUsuario, Modificado, FechaAct"
            )
            
            f_insc = self._get_val(row, 'FechaInscripcion')
            f_insc_str = format_sql_date(f_insc) if f_insc else "2000-01-01 00:00:00"
            
            vals = (
                f"{id_rec}, {self.id_sucursal}, "
                f"'{self._get_val(row, 'Nombres')}', "
                f"'{self._get_val(row, 'Apellidos')}', "
                f"'{c_email}', '{c_tel}', '{c_cel}', "
                f"'{self._get_val(row, 'Edad')}', "
                f"'{self._get_val(row, 'Fuente')}', "
                f"{self._get_val(row, 'OtroGimnasio')}, "
                f"'{self._get_val(row, 'CualGimnasio')}', "
                f"'{self._get_val(row, 'Paquete')}', "
                f"'{f_insc_str}', "
                f"{self._get_val(row, 'IdMovimiento')}, "
                f"'{format_sql_date(self._get_val(row, 'FechaRecorrido'))}', "
                f"{self._get_val(row, 'Status')}, "
                f"{self._get_val(row, 'Visitas')}, "
                f"{self._get_val(row, 'IdUsuario')}, "
                f"{self._get_val(row, 'Modificado')}, NOW()"
            )
            
            self._execute_remote(remote_cur, f"INSERT INTO tblRecorridos({fields}) VALUES ({vals})")
            self._execute_local(local_cur, f"UPDATE tblRecorridos SET Modificado = 0 WHERE IdRecorrido = {id_rec}")
        self.db.local_conn.commit()

    def push_aperturas(self, local_cur, remote_cur):
        logging.info("Respaldando Aperturas (Simulado)...")
        try:
            self._execute_local(local_cur, "SELECT * FROM tblAperturas WHERE Modificado = 1")
            rows = local_cur.fetchall()
            for row in rows:
                id_ap = self._get_val(row, 'IdApertura')
                fields = "IdApertura, IdSucursal, IdUsuario, FechaApertura, MontoInicial, Status, FechaAct"
                vals = f"{id_ap}, {self.id_sucursal}, {self._get_val(row, 'IdUsuario')}, '{format_sql_date(self._get_val(row, 'FechaApertura'))}', {self._get_val(row, 'MontoInicial')}, {self._get_val(row, 'Status')}, NOW()"
                self._execute_remote(remote_cur, f"REPLACE INTO tblAperturas({fields}) VALUES({vals})")
                self._execute_local(local_cur, f"UPDATE tblAperturas SET Modificado = 0 WHERE IdApertura = {id_ap}")
            self.db.local_conn.commit()
        except Exception:
            logging.debug("Tabla tblAperturas no disponible o no modificada.")

    def push_inventarios(self, local_cur, remote_cur):
        logging.info("Respaldando Inventarios...")
        self._execute_local(local_cur, "SELECT * FROM tblInventarios WHERE Modificado = 1 ORDER BY IdApertura")
        rows = local_cur.fetchall()
        for row in tqdm(rows, desc="Push Inv.", disable=sys.stdout is None):
            id_inv = self._get_val(row, 'IdInventario')
            
            fields = "IdInventario, IdApertura, IdCuota, ExiInicial, ExiAnterior, Entradas, Salidas, IdSucursal"
            vals = (
                f"{id_inv}, "
                f"{self._get_val(row, 'IdApertura')}, "
                f"{self._get_val(row, 'IdCuota')}, "
                f"{valida_nulo(self._get_val(row, 'ExiInicial'))}, "
                f"{valida_nulo(self._get_val(row, 'ExiAnterior'))}, "
                f"{valida_nulo(self._get_val(row, 'Entradas'))}, "
                f"{valida_nulo(self._get_val(row, 'Salidas'))}, "
                f"{self.id_sucursal}"
            )
            
            self._execute_remote(remote_cur, f"REPLACE INTO tblInventarios({fields}) VALUES ({vals})")
            self._execute_local(local_cur, f"UPDATE tblInventarios SET Modificado = 0 WHERE IdInventario = {id_inv}")
        self.db.local_conn.commit()

    def push_alertas(self, local_cur, remote_cur):
        logging.info("Respaldando Alertas...")
        self._execute_local(local_cur, "SELECT * FROM tblAlertas WHERE Modificado = 1")
        rows = local_cur.fetchall()
        for row in tqdm(rows, desc="Push Alertas", disable=sys.stdout is None):
            id_alerta = self._get_val(row, 'IdAlerta')
            
            fields = "IdAlerta, Alerta, FechaAlerta, IdSocio, IdUsuario, IdSucursal, IdSucursalSocio"
            vals = (
                f"{id_alerta}, "
                f"'{self._get_val(row, 'Alerta')}', "
                f"'{format_sql_date(self._get_val(row, 'FechaAlerta'))}', "
                f"{self._get_val(row, 'IdSocio')}, "
                f"{self._get_val(row, 'IdUsuario')}, "
                f"{self.id_sucursal}, "
                f"{valida_nulo(self._get_val(row, 'IdSucursalSocio'))}"
            )
            
            self._execute_remote(remote_cur, f"REPLACE INTO tblAlertas({fields}) VALUES ({vals})")
            self._execute_local(local_cur, f"UPDATE tblAlertas SET Modificado = 0 WHERE IdAlerta = {id_alerta}")
        self.db.local_conn.commit()

    def push_movimientos(self, local_cur, remote_cur):
        logging.info("Respaldando Movimientos...")
        self._execute_local(local_cur, "SELECT * FROM tblMovimientos WHERE Modificado = 1 ORDER BY IdMovimiento")
        rows = local_cur.fetchall()
        for row in tqdm(rows, desc="Push Movimientos", disable=sys.stdout is None):
            id_mov = self._get_val(row, 'IdMovimiento')
            
            vl_fp = str(valida_nulo(self._get_val(row, 'FormaPago'), True)).replace("'", "").replace("{", "").replace("}", "")

            fields = (
                "IdMovimiento, FolioMovimiento, FechaMovimiento, Pago, Total, IdSocio, IdSucursalSocio, Status, "
                "IdUsuario, IdCajero, FechaAct, IdSucursal, IdApertura, TotalDescuentos, TotalCargos, Efectivo, TarjetaDebito, TarjetaCredito, FormaPago"
            )
            
            vals = (
                f"{id_mov}, "
                f"'{self._get_val(row, 'FolioMovimiento')}', "
                f"'{format_sql_date(self._get_val(row, 'FechaMovimiento'))}', "
                f"{valida_nulo(self._get_val(row, 'Pago'))}, "
                f"{valida_nulo(self._get_val(row, 'Total'))}, "
                f"{valida_nulo(self._get_val(row, 'IdSocio'))}, "
                f"{valida_nulo(self._get_val(row, 'IdSucursalSocio'))}, "
                f"{self._get_val(row, 'Status')}, "
                f"{valida_nulo(self._get_val(row, 'IdUsuario'))}, "
                f"{valida_nulo(self._get_val(row, 'IdCajero'))}, "
                f"NOW(), {self.id_sucursal}, "
                f"{self._get_val(row, 'IdApertura')}, "
                f"{valida_nulo(self._get_val(row, 'TotalDescuentos'))}, "
                f"{valida_nulo(self._get_val(row, 'TotalCargos'))}, "
                f"{valida_nulo(self._get_val(row, 'Efectivo'))}, "
                f"{valida_nulo(self._get_val(row, 'TarjetaDebito'))}, "
                f"{valida_nulo(self._get_val(row, 'TarjetaCredito'))}, "
                f"'{vl_fp}'"
            )
            
            self._execute_remote(remote_cur, f"REPLACE INTO tblMovimientos({fields}) VALUES ({vals})")
            self._execute_local(local_cur, f"UPDATE tblMovimientos SET Modificado = 0 WHERE IdMovimiento = {id_mov}")
        self.db.local_conn.commit()

    def push_detalle_movimientos(self, local_cur, remote_cur):
        logging.info("Respaldando Detalles de Movimientos...")
        self._execute_local(local_cur, "SELECT * FROM tblDetalleMovimientos WHERE Modificado = 1 ORDER BY IdDetalleMovimiento")
        rows = local_cur.fetchall()
        for row in tqdm(rows, desc="Push Det. Mov.", disable=sys.stdout is None):
            id_det = self._get_val(row, 'IdDetalleMovimiento')
            
            self._execute_remote(remote_cur, f"DELETE FROM tblDetalleMovimientos WHERE IdDetalleMovimiento = {id_det} AND IdSucursal = {self.id_sucursal}")
            
            fields = (
                "IdDetalleMovimiento, IdMovimiento, IdCuota, Precio, PrecioDescuento, Puntos, FechaInicio, FechaFin, "
                "TipoVigencia, Vigencia, IdSocio, TipoCuota, Pago, Abono, Parcialidad, DescripcionCuota, IdMovimientoAnterior, "
                "IdMovimientoPosterior, IdDetalleMovimientoAbono, Locker, Cantidad, IdSucursal, IdSucursalSocio, Cargo, "
                "Descuento, IVA, Periodo, FechaAct, ConceptoCargo, ConceptoDescuento"
            )
            
            f_ini = self._get_val(row, 'FechaInicio')
            f_ini_str = format_sql_date(f_ini)[:10] if f_ini else "2000-01-01"
            f_fin = self._get_val(row, 'FechaFin')
            f_fin_str = format_sql_date(f_fin)[:10] if f_fin else "2000-01-01"
            
            desc_cuota = str(valida_nulo(self._get_val(row, 'DescripcionCuota'), True)).replace("'", "")

            vals = (
                f"{id_det}, "
                f"{self._get_val(row, 'IdMovimiento')}, "
                f"{self._get_val(row, 'IdCuota')}, "
                f"{self._get_val(row, 'Precio')}, "
                f"{valida_nulo(self._get_val(row, 'PrecioDescuento'))}, "
                f"{self._get_val(row, 'Puntos')}, "
                f"'{f_ini_str}', "
                f"'{f_fin_str}', "
                f"{self._get_val(row, 'TipoVigencia')}, "
                f"{self._get_val(row, 'Vigencia')}, "
                f"{valida_nulo(self._get_val(row, 'IdSocio'))}, "
                f"{self._get_val(row, 'TipoCuota')}, "
                f"{valida_nulo(self._get_val(row, 'Pago'))}, "
                f"{valida_nulo(self._get_val(row, 'Abono'))}, "
                f"{valida_nulo(self._get_val(row, 'Parcialidad'))}, "
                f"'{desc_cuota}', "
                f"{valida_nulo(self._get_val(row, 'IdMovimientoAnterior'))}, "
                f"{valida_nulo(self._get_val(row, 'IdMovimientoPosterior'))}, "
                f"{valida_nulo(self._get_val(row, 'IdDetalleMovimientoAbono'))}, "
                f"'{valida_nulo(self._get_val(row, 'Locker'), True)}', "
                f"{valida_nulo(self._get_val(row, 'Cantidad'))}, "
                f"{self.id_sucursal}, "
                f"{valida_nulo(self._get_val(row, 'IdSucursalSocio'))}, "
                f"{valida_nulo(self._get_val(row, 'Cargo'))}, "
                f"{valida_nulo(self._get_val(row, 'Descuento'))}, "
                f"{valida_nulo(self._get_val(row, 'Iva'))}, "
                f"'{valida_nulo(self._get_val(row, 'Periodo'), True)}', "
                f"NOW(), "
                f"'{valida_nulo(self._get_val(row, 'ConceptoCargo'), True)}', "
                f"'{valida_nulo(self._get_val(row, 'ConceptoDescuento'), True)}'"
            )
            
            self._execute_remote(remote_cur, f"INSERT INTO tblDetalleMovimientos({fields}) VALUES ({vals})")
            self._execute_local(local_cur, f"UPDATE tblDetalleMovimientos SET Modificado = 0 WHERE IdDetalleMovimiento = {id_det}")
        self.db.local_conn.commit()


    def push_movimientos_web_local(self, local_cur, remote_cur):
        logging.info("Respaldando Movimientos Web...")
        self._execute_local(local_cur, "SELECT * FROM tblMovimientosWeb WHERE Modificado = 1 ORDER BY IdMovimientoWeb")
        rows = local_cur.fetchall()
        for row in tqdm(rows, desc="Push Mov. Web", disable=sys.stdout is None):
            id_web = self._get_val(row, 'IdMovimientoWeb')
            sesiones = self._get_val(row, 'Sesiones')
            
            sql_upd = f"UPDATE tblMovimientosWeb SET Sesiones = {sesiones}, FechaAct = NOW() WHERE IdMovimientoWeb = {id_web}"
            self._execute_remote(remote_cur, sql_upd)
            self._execute_local(local_cur, f"UPDATE tblMovimientosWeb SET Modificado = 0 WHERE IdMovimientoWeb = {id_web}")
        self.db.local_conn.commit()

    def push_visitas_sesiones(self, local_cur, remote_cur):
        logging.info("Respaldando Visitas Sesiones...")
        try:
            self._execute_local(local_cur, "SELECT * FROM tblVisitasSesiones WHERE Modificado = 1 ORDER BY IdMovimientoWeb, IdDetalleMovimiento")
            rows = local_cur.fetchall()
            for row in tqdm(rows, desc="Push Visitas", disable=sys.stdout is None):
                id_web = self._get_val(row, 'IdMovimientoWeb')
                id_det = self._get_val(row, 'IdDetalleMovimiento')
                id_suc = self._get_val(row, 'IdSucursal')
                sesion = self._get_val(row, 'Sesion')
                id_soc = self._get_val(row, 'IdSocio')
                id_suc_soc = self._get_val(row, 'IdSucursalSocio')
                
                fields = "IdMovimientoWeb, IdDetalleMovimiento, IdSucursal, Sesion, IdSocio, IdSucursalSocio, FechaVisita, FechaAct"
                vals = (
                    f"{id_web}, {id_det}, {id_suc}, {sesion}, {id_soc}, {id_suc_soc}, "
                    f"'{format_sql_date(self._get_val(row, 'FechaVisita'))}', NOW()"
                )
                
                self._execute_remote(remote_cur, f"REPLACE INTO tblVisitasSesiones({fields}) VALUES ({vals})")
                self._execute_local(local_cur, f"UPDATE tblVisitasSesiones SET Modificado = 0 WHERE IdMovimientoWeb = {id_web} AND IdDetalleMovimiento = {id_det} AND Sesion = {sesion}")
            self.db.local_conn.commit()
        except Exception as e:
            logging.debug(f"Visitas sesiones falló o no existe: {e}")

    def push_visitas(self, local_cur, remote_cur):
        logging.info("Respaldando Visitas...")
        try:
            self._execute_local(local_cur, "SELECT * FROM tblVisitas WHERE Modificado >= 1 ORDER BY IdVisita")
            rows = local_cur.fetchall()
            if not rows:
                return

            vl_id_visita_inicio = self._get_val(rows[0], 'IdVisita')
            vl_id_visita_fin = None

            for row in tqdm(rows, desc="Push Visitas", disable=sys.stdout is None):
                id_visita = self._get_val(row, 'IdVisita')
                id_socio = self._get_val(row, 'IdSocio')
                id_suc_socio = self._get_val(row, 'IdSucursalSocio')
                fecha_visita = format_sql_date(self._get_val(row, 'FechaVisita'))
                es_salida = self._get_val(row, 'EsSalida')
                id_lector = self._get_val(row, 'IdLector')
                
                if not self.es_biostar:
                    fields = "IdVisita, IdSocio, IdSucursalSocio, FechaVisita, IdSucursal, EsSalida, IdLector"
                    vals = (
                        f"{id_visita}, "
                        f"{valida_nulo(id_socio)}, "
                        f"{valida_nulo(id_suc_socio)}, "
                        f"'{fecha_visita}', "
                        f"{self.id_sucursal}, "
                        f"{valida_nulo(es_salida)}, "
                        f"{valida_nulo(id_lector)}"
                    )
                    sql = f"REPLACE INTO tblVisitas({fields}) VALUES ({vals})"
                    self._execute_remote(remote_cur, sql)
                else:
                    id_visita_biostar = self._get_val(row, 'IdVisitaBioStar')
                    socio = self._get_val(row, 'Socio')
                    
                    # Delete first from remote
                    sql_del = f"DELETE FROM tblVisitas WHERE IdVisitaBioStar = {valida_nulo(id_visita_biostar)}"
                    self._execute_remote(remote_cur, sql_del)
                    
                    fields = "IdVisita, IdSocio, IdSucursalSocio, FechaVisita, IdSucursal, EsSalida, IdLector, Socio, IdVisitaBioStar"
                    vals = (
                        f"{id_visita}, "
                        f"{valida_nulo(id_socio)}, "
                        f"{valida_nulo(id_suc_socio)}, "
                        f"'{fecha_visita}', "
                        f"{self.id_sucursal}, "
                        f"{valida_nulo(es_salida)}, "
                        f"{valida_nulo(id_lector)}, "
                        f"'{valida_nulo(socio, True)}', "
                        f"{valida_nulo(id_visita_biostar)}"
                    )
                    sql = f"REPLACE INTO tblVisitas({fields}) VALUES ({vals})"
                    self._execute_remote(remote_cur, sql)
                
                vl_id_visita_fin = id_visita

            # Update modificado locally
            if vl_id_visita_fin is not None:
                sql_upd = f"UPDATE tblVisitas SET Modificado = 0 WHERE IdVisita >= {vl_id_visita_inicio} AND IdVisita <= {vl_id_visita_fin}"
                self._execute_local(local_cur, sql_upd)
                self.db.local_conn.commit()

        except Exception as e:
            logging.error(f"Error en push_visitas: {e}")

    def push_asistencias(self, local_cur, remote_cur):
        logging.info("Respaldando Asistencias...")
        try:
            self._execute_local(local_cur, "SELECT * FROM tblAsistencias WHERE Modificado >= 1")
            rows = local_cur.fetchall()
            for row in tqdm(rows, desc="Push Asistencias", disable=sys.stdout is None):
                id_asistencia = self._get_val(row, 'IdAsistencia')
                id_usuario = self._get_val(row, 'IdUsuario')
                fecha_asistencia = format_sql_date(self._get_val(row, 'FechaAsistencia'))
                rechazo_huella = self._get_val(row, 'RechazoHuella')
                es_salida = self._get_val(row, 'EsSalida')
                id_lector = self._get_val(row, 'IdLector')

                fields = "IdAsistencia, IdUsuario, FechaAsistencia, RechazoHuella, FechaAct, IdSucursal, EsSalida, IdLector"
                vals = (
                    f"{id_asistencia}, "
                    f"{id_usuario}, "
                    f"'{fecha_asistencia}', "
                    f"{rechazo_huella}, "
                    f"NOW(), "
                    f"{self.id_sucursal}, "
                    f"{valida_nulo(es_salida)}, "
                    f"{valida_nulo(id_lector)}"
                )
                self._execute_remote(remote_cur, f"REPLACE INTO tblAsistencias({fields}) VALUES ({vals})")
                self._execute_local(local_cur, f"UPDATE tblAsistencias SET Modificado = 0 WHERE IdAsistencia = {id_asistencia}")
            self.db.local_conn.commit()
        except Exception as e:
            logging.error(f"Error en push_asistencias: {e}")

    def push_fotos(self, local_cur, remote_cur):
        import os
        logging.info("Respaldando Fotos...")
        try:
            sql = (
                "SELECT A.*, Serie FROM tblSocios A "
                "INNER JOIN tblSucursales B ON A.IdSucursal = B.IdSucursal "
                "WHERE ModificadoFoto = 1 ORDER BY IdSocio"
            )
            self._execute_local(local_cur, sql)
            rows = local_cur.fetchall()
            for row in tqdm(rows, desc="Push Fotos", disable=sys.stdout is None):
                id_socio = self._get_val(row, 'IdSocio')
                id_suc = self._get_val(row, 'IdSucursal')
                es_jpg = self._get_val(row, 'EsJpg')
                serie = self._get_val(row, 'Serie')

                # Update remote older photos status
                self._execute_remote(remote_cur, f"UPDATE tblSociosFotos SET EsUltimaFoto = 0 WHERE IdSocio = {id_socio} AND IdSucursal = {id_suc}")

                # Insert remote placeholder
                sql_ins_placeholder = (
                    "INSERT INTO tblSociosFotos(IdSocio, IdSucursal, IdSucursalActualiza, FechaAct, EsUltimaFoto, EsJpg) "
                    f"VALUES({id_socio}, {id_suc}, {self.id_sucursal}, NOW(), 1, {es_jpg})"
                )
                self._execute_remote(remote_cur, sql_ins_placeholder)

                # Remote recipients sync
                sql_rep_rec = (
                    "REPLACE INTO tblSociosSucursalesRecepcion(IdSocio, IdSucursal, IdSucursalRecepcion, IdSucursalActualiza, Recepcion, FechaAct, TipoRecepcion) "
                    f"SELECT {id_socio}, {id_suc}, IdSucursal, {self.id_sucursal}, 0, NOW(), 1 FROM tblSucursales WHERE Status = 0 AND IdSucursal <> {self.id_sucursal}"
                )
                self._execute_remote(remote_cur, sql_rep_rec)

                # Local photo path
                ext = ".jpg" if es_jpg == 1 else ".bmp"
                file_name = f"{serie}{id_socio}{ext}"
                local_path = os.path.join(self.ruta_servidor, "Fotos", file_name)

                # Check and read file binary
                if os.path.exists(local_path):
                    with open(local_path, "rb") as f:
                        foto_binary = f.read()

                    # Update binary photo remote
                    sql_upd_foto = (
                        "UPDATE tblSociosFotos SET Foto = %s "
                        f"WHERE IdSocio = {id_socio} AND IdSucursal = {id_suc} AND EsUltimaFoto = 1"
                    )
                    self._execute_remote(remote_cur, sql_upd_foto, (foto_binary,))
                else:
                    logging.warning(f"Foto no encontrada localmente: {local_path}")

                # Clear local ModificadoFoto flag
                self._execute_local(local_cur, f"UPDATE tblSocios SET ModificadoFoto = 0 WHERE IdSocio = {id_socio} AND IdSucursal = {id_suc}")
            
            self.db.local_conn.commit()
        except Exception as e:
            logging.error(f"Error en push_fotos: {e}")

    def push_huellas(self, local_cur, remote_cur):
        import os
        logging.info("Respaldando Huellas...")
        try:
            # 1. Remote clean up for empty/null fingerprint receipt flags
            sql_clean = (
                "UPDATE tblSociosHuellas A "
                "INNER JOIN tblSociosSucursalesRecepcion B ON A.IdSocio = B.IdSocio AND A.IdSucursal = B.IdSucursal AND A.IdSucursalActualiza = B.IdSucursalActualiza "
                f"SET Recepcion = 1 WHERE IdSucursalRecepcion = {self.id_sucursal} AND Recepcion = 0 AND TipoRecepcion = 2 "
                f"AND A.IdSucursalActualiza <> {self.id_sucursal} AND Huella IS NULL"
            )
            try:
                self._execute_remote(remote_cur, sql_clean)
            except Exception as e:
                logging.debug(f"Remoto clean up tblSociosHuellas omitido o error: {e}")

            # 2. Query local modified fingerprints
            sql = (
                "SELECT A.*, Serie FROM tblSociosHuellas A "
                "INNER JOIN tblSucursales B ON A.IdSucursal = B.IdSucursal "
                "WHERE ModificadoHuella = 1 ORDER BY IdSocio"
            )
            self._execute_local(local_cur, sql)
            rows = local_cur.fetchall()
            
            for row in tqdm(rows, desc="Push Huellas", disable=sys.stdout is None):
                id_socio = self._get_val(row, 'IdSocio')
                id_suc = self._get_val(row, 'IdSucursal')
                serie = self._get_val(row, 'Serie')

                # Update older fingerprints remote
                self._execute_remote(remote_cur, f"UPDATE tblSociosHuellas SET EsUltimaHuella = 0 WHERE EsEmpleado = 0 AND IdSocio = {id_socio} AND IdSucursal = {id_suc}")

                # Insert remote placeholder
                sql_ins_placeholder = (
                    "INSERT INTO tblSociosHuellas(IdSocio, IdSucursal, IdSucursalActualiza, FechaAct, EsUltimaHuella, EsEmpleado, Status) "
                    f"VALUES({id_socio}, {id_suc}, {self.id_sucursal}, NOW(), 1, 0, 2)"
                )
                self._execute_remote(remote_cur, sql_ins_placeholder)

                # Remote recipients sync
                sql_rep_rec = (
                    "REPLACE INTO tblSociosSucursalesRecepcion(IdSocio, IdSucursal, IdSucursalRecepcion, IdSucursalActualiza, Recepcion, FechaAct, TipoRecepcion) "
                    f"SELECT {id_socio}, {id_suc}, IdSucursal, {self.id_sucursal}, 0, NOW(), 2 FROM tblSucursales WHERE Status = 0 AND IdSucursal <> {self.id_sucursal}"
                )
                self._execute_remote(remote_cur, sql_rep_rec)

                # Local fingerprint path
                file_name = f"{serie}{id_socio}.fpt"
                local_path = os.path.join(self.ruta_servidor, "Huellas", file_name)

                # Check and read file binary
                if os.path.exists(local_path):
                    with open(local_path, "rb") as f:
                        huella_binary = f.read()

                    # Update binary fingerprint remote
                    sql_upd_huella = (
                        "UPDATE tblSociosHuellas SET Huella = %s "
                        f"WHERE EsEmpleado = 0 AND IdSocio = %s AND IdSucursal = %s AND EsUltimaHuella = 1"
                    )
                    self._execute_remote(remote_cur, sql_upd_huella, (huella_binary, id_socio, id_suc))
                else:
                    logging.warning(f"Huella no encontrada localmente: {local_path}")

                # Clear local flags
                self._execute_local(local_cur, f"UPDATE tblSociosHuellas SET ModificadoHuella = 0 WHERE IdSocio = {id_socio} AND IdSucursal = {id_suc}")
                self._execute_local(local_cur, f"UPDATE tblSocios SET ModificadoHuella = 0 WHERE IdSocio = {id_socio} AND IdSucursal = {id_suc}")

            self.db.local_conn.commit()
        except Exception as e:
            logging.error(f"Error en push_huellas: {e}")



