import numpy as np
import os
import pandas as pd
from PyQt5.QtGui import QFont, QPixmap, QStandardItemModel, QStandardItem, QColor, QIcon
from PyQt5.QtCore import Qt, pyqtSlot, QThread, pyqtSignal, QRect, QRegExp, QMetaObject, QEvent, QSignalMapper, QCoreApplication, QSize, QPoint, QSortFilterProxyModel
from PyQt5.QtWidgets import QHBoxLayout, QDialogButtonBox, QRadioButton, QDialog, QFileDialog, QApplication, QMainWindow, QCheckBox, QWidget, QPushButton, QMessageBox, QTableWidgetItem, QLabel, QTabWidget, QProgressBar, QDesktopWidget, QSizePolicy, QGroupBox, QFormLayout, QGridLayout, QSpacerItem, QLineEdit, QTableWidget, QListWidget, QAction, QMenu, QComboBox, QMenuBar, QTableView, QStatusBar, QVBoxLayout, QTextEdit, QFrame
from datetime import datetime as datetime
from pysftp import Connection as pysftpConnection
from pysftp import CnOpts as pysftpCnOpts
from shutil import rmtree as shutilrmtree
from tempfile import TemporaryDirectory as tempfileTemporaryDirectory
from os.path import join as ospathjoin
from imp import reload as impreload
from sys import path as syspath
from warnings import filterwarnings
from scripts.modules import login_resources
from scripts.modules import resources
from scripts.modules import FunVal # Funciones Validador
from scripts.modules import config # Credenciales
from scripts.modules.sql import Usuarios

########################################################################
# Inputs globales
########################################################################
global base_sql, table_sql, pass_sql, text_user

filterwarnings("ignore")
base_sql = "EOD202012"
table_sql = "EOD202012"
credentials = config.credentials()
pass_sql = credentials.pass_sql
text_user = credentials.text_user

########################################################################
# Clase para threading
########################################################################


class External(QThread):

    def __init__(self, parent=None):
        super(External, self).__init__(parent)

    """
    Runs a counter thread.
    """
    countChanged = pyqtSignal(int)
    result = pyqtSignal(str)
    df = pyqtSignal(pd.core.frame.DataFrame)

    # Actualiza libreria prueba546 desde FTP

    f = tempfileTemporaryDirectory()

    myHostname = "162.243.165.69"
    myUsername = "root"
    myPassword = "DatosCMD2019"
    filename = "ErroresValidador2_web.py"
    opts = pysftpCnOpts()
    opts.hostkeys = None
    with pysftpConnection(host=myHostname, username=myUsername, password=myPassword, cnopts=opts) as sftp:
        remoteFilePath = '/root/Validador/' + filename
        localFilePath = ospathjoin(f.name, filename)
        sftp.get(remoteFilePath, localFilePath)

    syspath.append(f.name)

    import ErroresValidador2_web

    impreload(ErroresValidador2_web)
    # from ErroresValidador2_web import UpdListError
    from ErroresValidador2_web import UpdListError2

    syspath.remove(f.name)

    try:
        shutilrmtree(f.name)
    except Exception:
        pass

    def run(self):
        # Descarga base de datos desde SQL
        df, result, self.parent().df_or = FunVal.DescargaUltSql(self, text_user, pass_sql, base_sql, table_sql, list_des=self.parent().list_descarga)
        self.result.emit(result)
        self.df.emit(df)

########################################################################
# Clase elementos gráficos de interfaz de usuario
########################################################################


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):

        ########################################################################
        # Define Ventana Principal
        ########################################################################
        sizeObject = QDesktopWidget().screenGeometry(-1)
        # print(" Screen size : "  + str(sizeObject.height()) + "x"  + str(sizeObject.width()))
        fac1 = sizeObject.width() / 1600 * 0.85
        fac2 = sizeObject.height() / 1000 * 0.85
        global factor
        factor = min(fac1, fac2)
        # factor = factor*0.95
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(1700 * factor, 1000 * factor)
        MainWindow.setMinimumSize(1600 * factor, 1000 * factor)

        sizePolicy = QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(MainWindow.sizePolicy().hasHeightForWidth())
        MainWindow.setSizePolicy(sizePolicy)
        MainWindow.setIconSize(QSize(19, 20))

        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")

        MainWindow.setCentralWidget(self.centralwidget)
        self.setAttribute(Qt.WA_DeleteOnClose)
        
        ########################################################################
        # Variables utilizadas en funciones
        ########################################################################

        # Dataframes

        # Dataframe auxiliar, para reporte
        self.reporte = pd.DataFrame()
        # Dataframe principal
        self.df = pd.DataFrame()
        # Dataframe secundario, contiene solo el interview__key buscado)
        self.df2 = pd.DataFrame()
        # Dataframe auxiliar, utilizado para construir QTableWidget
        self.df3 = pd.DataFrame()
        # Variables

        # Input editable desde recuadro de lista de errores para rev codificacion
        self.RevCod1 = 0
        # Threshold
        self.threshold = 0
        # Indicador de sobrepaso de threshold para función oncellchanged
        self.i = 0
        # Interview Key buscado
        self.key = ""
        # Condicion para entrar a boton actualiza errores
        self.EntraActualiza = 1
        # index para el filtro del texto
        self.indexcombobox = 0
        # => DIego 23/06
        self.df_Errores = pd.DataFrame()
        # => DIego 23/06

        # Username 
        self.username = None

        # Diego 19/07/2020
        self.list_descarga = [1, 4]
        # Diego 19/07/2020

        ########################################################################
        # Pestañas, imágenes y tabla
        ########################################################################

        # Layout que contiene pestañas, imagenes y tabla
        self.gridLayoutWidget_6 = QWidget(self.centralwidget)
        self.gridLayoutWidget_6.setGeometry(QRect(40 * factor, 100 * factor, 1125 * factor, 871 * factor))
        self.gridLayoutWidget_6.setObjectName("gridLayoutWidget_6")
        self.grid_imagenes = QGridLayout(self.gridLayoutWidget_6)
        self.grid_imagenes.setContentsMargins(0, 0, 0, 0)
        self.grid_imagenes.setObjectName("grid_imagenes")

        # Tabla donde se muestra información de encuestas
        self.tableWidget = QTableWidget(self.gridLayoutWidget_6)
        sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.tableWidget.sizePolicy().hasHeightForWidth())
        self.tableWidget.setSizePolicy(sizePolicy)
        self.tableWidget.setMinimumSize(QSize(0, 250 * factor))
        self.tableWidget.setRowCount(0)  # Numero de columna debería quedar en función del numero de personas
        self.tableWidget.setColumnCount(20)  # Varia por hoja
        self.tableWidget.setObjectName("tabla")  # nombre de la tabla
        self.tableWidget.setFont(QFont("Arial", int(9)))
        self.tableWidget.setColumnWidth(0, 114 * factor)
        self.tableWidget.setColumnWidth(1, 57 * factor)
        self.tableWidget.setColumnWidth(2, 39 * factor)
        self.tableWidget.setColumnWidth(3, 39 * factor)
        self.tableWidget.setColumnWidth(4, 67 * factor)
        self.tableWidget.setColumnWidth(5, 57 * factor)
        self.tableWidget.setColumnWidth(6, 95 * factor)
        self.tableWidget.setColumnWidth(7, 48 * factor)
        self.tableWidget.setColumnWidth(8, 48 * factor)
        self.tableWidget.setColumnWidth(9, 39 * factor)
        self.tableWidget.setColumnWidth(10, 39 * factor)
        self.tableWidget.setColumnWidth(11, 39 * factor)
        self.tableWidget.setColumnWidth(12, 39 * factor)
        self.tableWidget.setColumnWidth(13, 39 * factor)
        self.tableWidget.setColumnWidth(14, 48 * factor)
        self.tableWidget.setColumnWidth(15, 57 * factor)
        self.tableWidget.setColumnWidth(16, 95 * factor)
        self.tableWidget.setColumnWidth(17, 57 * factor)
        self.tableWidget.setColumnWidth(18, 67 * factor)
        self.tableWidget.setColumnWidth(19, 48 * factor)
        self.tableWidget.cellChanged.connect(self.onCellChanged)
        self.tableWidget.horizontalHeader().setFont(QFont("Arial", int(9), QFont.Bold))
        self.tableWidget.verticalHeader().setVisible(False)
        self.grid_imagenes.addWidget(self.tableWidget, 1, 0, 1, 1)

        ####################
        # Widget de pestañas
        ####################

        self.tabWidget = QTabWidget(self.gridLayoutWidget_6)
        self.tabWidget.setObjectName("tabWidget")

        #############
        # Pestaña n°1
        #############

        # Creamos pestaña n°1
        self.tab_v1 = QWidget()
        self.tab_v1.setObjectName("tab_v1")
        # Layout que contiene a imagen en pestaña n°1
        self.verticalLayoutWidget_3 = QWidget(self.tab_v1)
        self.verticalLayoutWidget_3.setGeometry(QRect(0, 0, 1121 * factor, 541 * factor))
        self.verticalLayoutWidget_3.setObjectName("verticalLayoutWidget_3")
        self.verticalLayout_2 = QVBoxLayout(self.verticalLayoutWidget_3)
        self.verticalLayout_2.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_2.setObjectName("verticalLayout_2")

        # Imagen en página 1
        self.imagen1 = QLabel(self.verticalLayoutWidget_3)
        self.imagen1.setObjectName("imagen1")
        self.imagen1.setText("")
        self.imagen1.setPixmap(QPixmap(':/images/h1.png'))
        self.imagen1.setScaledContents(True)

        # Agregamos imagen a layout, y pestaña al widget de pestañas
        self.verticalLayout_2.addWidget(self.imagen1)
        self.tabWidget.addTab(self.tab_v1, "Hoja 1")

        #############
        # Pestaña n°2
        #############

        # Creamos pestaña n°2
        self.tab_v2 = QWidget()
        self.tab_v2.setObjectName("tab_v2")
        # Layout que contiene a imagen en pestaña n°2
        self.verticalLayoutWidget_4 = QWidget(self.tab_v2)
        self.verticalLayoutWidget_4.setGeometry(QRect(0, 0, 1121 * factor, 541 * factor))
        self.verticalLayoutWidget_4.setObjectName("verticalLayoutWidget_4")
        self.verticalLayout_3 = QVBoxLayout(self.verticalLayoutWidget_4)
        self.verticalLayout_3.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_3.setObjectName("verticalLayout_3")

        # Imagen en página 2
        self.imagen2 = QLabel(self.verticalLayoutWidget_4)
        self.imagen2.setObjectName("imagen2")
        self.imagen2.setText("")
        self.imagen2.setPixmap(QPixmap(':/images/h2.png'))
        self.imagen2.setScaledContents(True)

        # Agregamos imagen a layout, y pestaña al widget de pestañas
        self.verticalLayout_3.addWidget(self.imagen2)
        self.tabWidget.addTab(self.tab_v2, "Hoja 2")

        #############
        # Pestaña n°3
        #############

        # Creamos pestaña n°3
        self.tab_v3 = QWidget()
        self.tab_v3.setObjectName("tab_v3")
        # Layout que contiene a imagen en pestaña n°3
        self.verticalLayoutWidget_5 = QWidget(self.tab_v3)
        self.verticalLayoutWidget_5.setGeometry(QRect(0, 0, 1121 * factor, 541 * factor))
        self.verticalLayoutWidget_5.setObjectName("verticalLayoutWidget_5")
        self.verticalLayout_4 = QVBoxLayout(self.verticalLayoutWidget_5)
        self.verticalLayout_4.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_4.setObjectName("verticalLayout_4")

        # Imagen en página 3
        self.imagen3 = QLabel(self.verticalLayoutWidget_5)
        self.imagen3.setObjectName("imagen3")
        self.imagen3.setText("")
        self.imagen3.setPixmap(QPixmap(':/images/h3.png'))
        self.imagen3.setScaledContents(True)
        self.imagen3.setObjectName("imagen3")

        # Agregamos imagen a layout, y pestaña al widget de pestañas
        self.verticalLayout_4.addWidget(self.imagen3)
        self.tabWidget.addTab(self.tab_v3, "Hoja 3")

        #############
        # Pestaña n°4
        #############

        # Creamos pestaña n°4
        self.tab_v4 = QWidget()
        self.tab_v4.setObjectName("tab_v4")
        # Layout que contiene a imagen en pestaña n°4
        self.verticalLayoutWidget_6 = QWidget(self.tab_v4)
        self.verticalLayoutWidget_6.setGeometry(QRect(0, 0, 1121 * factor, 541 * factor))
        self.verticalLayoutWidget_6.setObjectName("verticalLayoutWidget_6")
        self.verticalLayout_5 = QVBoxLayout(self.verticalLayoutWidget_6)
        self.verticalLayout_5.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_5.setObjectName("verticalLayout_5")

        # Imagen en página 4
        self.imagen4 = QLabel(self.verticalLayoutWidget_5)
        self.imagen4.setObjectName("imagen4")
        self.imagen4.setText("")
        self.imagen4.setPixmap(QPixmap(':/images/h4.png'))
        self.imagen4.setScaledContents(True)

        # Agregamos imagen a layout, y pestaña al widget de pestañas
        self.verticalLayout_5.addWidget(self.imagen4)
        self.tabWidget.addTab(self.tab_v4, "Hoja 4")

        #############
        # Pestaña n°5
        #############

        # Creamos pestaña n°5
        self.tab_v5 = QWidget()
        self.tab_v5.setObjectName("tab_v5")
        # Layout que contiene a imagen en pestaña n°2
        self.verticalLayoutWidget_7 = QWidget(self.tab_v5)
        self.verticalLayoutWidget_7.setGeometry(QRect(0, 0, 1121 * factor, 541 * factor))
        self.verticalLayoutWidget_7.setObjectName("verticalLayoutWidget_7")
        self.verticalLayout_6 = QVBoxLayout(self.verticalLayoutWidget_7)
        self.verticalLayout_6.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_6.setObjectName("verticalLayout_6")

        # Imagen en página 5
        self.imagen5 = QLabel(self.verticalLayoutWidget_4)
        self.imagen5.setObjectName("imagen5")
        self.imagen5.setText("")
        self.imagen5.setPixmap(QPixmap(':/images/h5.png'))
        self.imagen5.setScaledContents(True)

        # Agregamos imagen a layout, y pestaña al widget de pestañas
        self.verticalLayout_6.addWidget(self.imagen5)
        self.tabWidget.addTab(self.tab_v5, "Hoja 5")

        #############
        # Pestaña n°6
        #############

        # Creamos pestaña n°6
        self.tab_v6 = QWidget()
        self.tab_v6.setObjectName("tab_v6")
        # Layout que contiene a imagen en pestaña n°6
        self.verticalLayoutWidget_8 = QWidget(self.tab_v6)
        self.verticalLayoutWidget_8.setGeometry(QRect(0, 0, 1121 * factor, 541 * factor))
        self.verticalLayoutWidget_8.setObjectName("verticalLayoutWidget_8")
        self.verticalLayout_7 = QVBoxLayout(self.verticalLayoutWidget_8)
        self.verticalLayout_7.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_7.setObjectName("verticalLayout_7")

        # Imagen en página 6
        self.imagen6 = QLabel(self.verticalLayoutWidget_7)
        self.imagen6.setObjectName("imagen6")
        self.imagen6.setText("")
        self.imagen6.setPixmap(QPixmap(':/images/h6.png'))
        self.imagen6.setScaledContents(True)

        # Agregamos imagen a layout, y pestaña al widget de pestañas
        self.verticalLayout_7.addWidget(self.imagen6)
        self.tabWidget.addTab(self.tab_v6, "Hoja 6")

        #######################################
        # Se agrega widget de pestañas a layout
        #######################################

        self.grid_imagenes.addWidget(self.tabWidget, 0, 0, 1, 1)

        #######################################
        # Botón para guardar
        #######################################

        self.guardar = QPushButton(self.gridLayoutWidget_6)
        self.guardar.setObjectName("guardar")
        self.guardar.setObjectName("guardar")
        self.guardar.setFont(QFont("Arial", int(9 * factor)))
        self.guardar.setEnabled(False)
        self.guardar.clicked.connect(self.GuardarCambios)
        self.grid_imagenes.addWidget(self.guardar, 2, 0, 1, 1, Qt.AlignRight)

        self.tabWidget.currentChanged.connect(self.cambio_hoja)
        self.tabWidget.setCurrentIndex(0)

        ########################################################################
        # Caja agrupadora y elementos
        ########################################################################
        
        # Caja
        self.groupBox = QGroupBox(self.centralwidget)
        self.groupBox.setGeometry(QRect(40 * factor, 10 * factor, 1600 * factor, 61 * factor))
        self.groupBox.setObjectName("interview_info")
        
        # Grid Layout en caja
        self.widget_interview_info = QWidget(self.groupBox)
        self.widget_interview_info.setGeometry(QRect(10 * factor, 20 * factor, 1580 * factor, 31 * factor))
        self.widget_interview_info.setObjectName("widget_interview_info")
        self.grid_interview_info = QGridLayout(self.widget_interview_info)
        self.grid_interview_info.setContentsMargins(0, 0, 0, 0)
        self.grid_interview_info.setObjectName("grid_interview_info")
        
        # Teléfono
        self.lineEdit_5 = QLineEdit(self.widget_interview_info)
        self.lineEdit_5.setObjectName("lineEdit_5")
        self.lineEdit_5.setStyleSheet("font: 13px;")
        self.grid_interview_info.addWidget(self.lineEdit_5, 0, 14, 1, 1)
        self.label_4 = QLabel(self.widget_interview_info)
        self.label_4.setObjectName("label_4")
        self.grid_interview_info.addWidget(self.label_4, 0, 13, 1, 1)

        # Coordinador
        self.lineEdit_4 = QLineEdit(self.widget_interview_info)
        self.lineEdit_4.setObjectName("lineEdit_4")
        self.lineEdit_4.setStyleSheet("font: 13px;")
        self.grid_interview_info.addWidget(self.lineEdit_4, 0, 12, 1, 1)
        self.label_2 = QLabel(self.widget_interview_info)
        self.label_2.setObjectName("label_2")
        self.grid_interview_info.addWidget(self.label_2, 0, 11, 1, 1)

        # Encuestador
        self.lineEdit_7 = QLineEdit(self.widget_interview_info)
        self.lineEdit_7.setObjectName("lineEdit_7")
        self.lineEdit_7.setStyleSheet("font: 13px;")
        self.grid_interview_info.addWidget(self.lineEdit_7, 0, 16, 1, 1)
        self.label_6 = QLabel(self.widget_interview_info)
        self.label_6.setObjectName("label_6")
        self.grid_interview_info.addWidget(self.label_6, 0, 15, 1, 1)

        # Segmento
        self.lineEdit_2 = QLineEdit(self.widget_interview_info)
        self.lineEdit_2.setObjectName("lineEdit_2")
        self.lineEdit_2.setStyleSheet("font: 13px;")
        self.grid_interview_info.addWidget(self.lineEdit_2, 0, 6, 1, 1)
        self.label_1 = QLabel(self.widget_interview_info)
        self.label_1.setObjectName("label_1")
        self.grid_interview_info.addWidget(self.label_1, 0, 5, 1, 1)

        # Linea divisoria
        self.line = QFrame(self.widget_interview_info)
        self.line.setFrameShape(QFrame.VLine)
        self.line.setFrameShadow(QFrame.Sunken)
        self.line.setObjectName("line")
        self.grid_interview_info.addWidget(self.line, 0, 3, 1, 1)

        # Hogar
        self.lineEdit_3 = QLineEdit(self.widget_interview_info)
        self.lineEdit_3.setObjectName("lineEdit_3")
        self.lineEdit_3.setStyleSheet("font: 13px;")
        self.grid_interview_info.addWidget(self.lineEdit_3, 0, 8, 1, 1)
        self.label_3 = QLabel(self.widget_interview_info)
        self.label_3.setObjectName("label_3")
        self.grid_interview_info.addWidget(self.label_3, 0, 7, 1, 1)
        
        # N° Personas Hogar
        self.lineEdit_8 = QLineEdit(self.widget_interview_info)
        self.lineEdit_8.setObjectName("lineEdit_8")
        self.lineEdit_8.setStyleSheet("font: 13px;")
        self.grid_interview_info.addWidget(self.lineEdit_8, 0, 10, 1, 1)
        self.label_8 = QLabel(self.widget_interview_info)
        self.label_8.setObjectName("label_8")
        self.grid_interview_info.addWidget(self.label_8, 0, 9, 1, 1)

        # Spacers        
        spacerItem = QSpacerItem(40, 20, QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.grid_interview_info.addItem(spacerItem, 0, 4, 1, 1)
        spacerItem1 = QSpacerItem(40, 20, QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.grid_interview_info.addItem(spacerItem1, 0, 2, 1, 1)

        ########################################################################
        # Buscar Interview Key
        ########################################################################

        # Boton para buscar

        self.cargar = QPushButton(self.widget_interview_info)
        self.cargar.setObjectName("cargar")
        self.cargar.setStyleSheet("font: 13px;")
        self.grid_interview_info.addWidget(self.cargar, 0, 0, 1, 1)
        self.cargar.clicked.connect(self.loadData)
        self.cargar.setEnabled(False)

        # self.cargar.clicked.connect(self.UpdateErrorsKey)
        # self.cargar.clicked.connect(self.UpdateComentariosKey)

        self.Buscar_Key = QLineEdit(self.widget_interview_info)
        self.Buscar_Key.setObjectName("Buscar_Key")
        self.Buscar_Key.setStyleSheet("font: 13px;")
        self.grid_interview_info.addWidget(self.Buscar_Key, 0, 1, 1, 1)
        self.Buscar_Key.setPlaceholderText("Ingresar Key")

        ########################################################################
        # Lista errores
        ########################################################################

        self.errores_comentarios = QTabWidget(self.centralwidget)
        self.errores_comentarios.setGeometry(QRect(1200 * factor, 100 * factor, 347 * factor, 301 * factor))
        self.errores_comentarios.setObjectName("errores_comentarios")
        self.tab_errores = QWidget()
        self.tab_errores.setObjectName("tab_errores")
        self.gridLayoutWidget_3 = QWidget(self.tab_errores)
        self.gridLayoutWidget_3.setGeometry(QRect(0, 0, 339 * factor, 271 * factor))
        self.gridLayoutWidget_3.setObjectName("gridLayoutWidget")
        self.layout_errores = QGridLayout(self.gridLayoutWidget_3)
        self.layout_errores.setContentsMargins(0, 0, 0, 0)
        self.layout_errores.setSpacing(2)
        self.layout_errores.setObjectName("layout_errores")

        self.listError = QTableView(self.gridLayoutWidget_3)
        self.listError.setObjectName("listWidget")
        self.layout_errores.addWidget(self.listError, 0, 0, 1, 1)
        self.listError.verticalHeader().setVisible(False)

        # self.listError = QListWidget(self.gridLayoutWidget_3)
        # self.listError.setObjectName("listWidget")
        # self.layout_errores.addWidget(self.listError, 0, 0, 1, 1)
        # => DIego 23/06

        # Botón actualiza errores
        self.UpdateErrors = QPushButton(self.gridLayoutWidget_3)
        self.UpdateErrors.setObjectName("UpdateErrors")
        self.UpdateErrors.setObjectName("Actualizar")
        self.UpdateErrors.setFont(QFont("Arial", int(9 * factor)))
        self.UpdateErrors.clicked.connect(self.UpdateErrorsKey)
        self.UpdateErrors.setEnabled(False)

        self.layout_errores.addWidget(self.UpdateErrors, 1, 0, 1, 1)
        self.errores_comentarios.addTab(self.tab_errores, "")
        self.tab_comentarios = QWidget()
        self.tab_comentarios.setObjectName("tab_comentarios")
        self.gridLayoutWidget_2 = QWidget(self.tab_comentarios)
        self.gridLayoutWidget_2.setGeometry(QRect(0, 0, 339 * factor, 271 * factor))
        self.gridLayoutWidget_2.setObjectName("gridLayoutWidget_2")
        self.layout_comentarios = QGridLayout(self.gridLayoutWidget_2)
        self.layout_comentarios.setContentsMargins(0, 0, 0, 0)
        self.layout_comentarios.setSpacing(2)
        self.layout_comentarios.setObjectName("layout_comentarios")
        self.agregar_comentario = QPushButton(self.gridLayoutWidget_2)
        self.agregar_comentario.setObjectName("agregar_comentario")
        self.agregar_comentario.clicked.connect(self.AgregarComentario)
        self.agregar_comentario.setEnabled(False)
        self.layout_comentarios.addWidget(self.agregar_comentario, 1, 0, 1, 2)
        self.viewComentarios = QTableView(self.gridLayoutWidget_2)
        self.viewComentarios.setObjectName("listWidget_2")
        self.layout_comentarios.addWidget(self.viewComentarios, 0, 0, 1, 2)
        self.viewComentarios.verticalHeader().setVisible(False)
        self.errores_comentarios.addTab(self.tab_comentarios, "")

        ########################################################################
        # Lista Encuestas por validar
        ########################################################################

        self.errores_comentarios_2 = QTabWidget(self.centralwidget)
        self.errores_comentarios_2.setGeometry(QRect(1200 * factor, 450 * factor, 347 * factor, 511 * factor))
        self.errores_comentarios_2.setObjectName("errores_comentarios_2")
        self.tab_errores_2 = QWidget()
        self.tab_errores_2.setObjectName("tab_errores_2")
        self.gridLayoutWidget_4 = QWidget(self.tab_errores_2)
        self.gridLayoutWidget_4.setGeometry(QRect(0, 0, 339 * factor, 491 * factor))
        self.gridLayoutWidget_4.setObjectName("gridLayoutWidget_4")
        self.layout_errores_2 = QGridLayout(self.gridLayoutWidget_4)
        self.layout_errores_2.setContentsMargins(0, 0, 0, 0)
        self.layout_errores_2.setSpacing(2)
        self.layout_errores_2.setObjectName("layout_errores_2")
        self.gridLayout_2 = QGridLayout()
        self.gridLayout_2.setObjectName("gridLayout_2")
        self.lineEdit = QLineEdit(self.gridLayoutWidget_4)
        self.lineEdit.setEnabled(True)
        sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(10)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.lineEdit.sizePolicy().hasHeightForWidth())
        self.lineEdit.setSizePolicy(sizePolicy)
        self.lineEdit.setObjectName("lineEdit")
        self.gridLayout_2.addWidget(self.lineEdit, 0, 1, 1, 1)
        self.comboBox = QComboBox(self.gridLayoutWidget_4)
        sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(10)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.comboBox.sizePolicy().hasHeightForWidth())
        self.comboBox.setSizePolicy(sizePolicy)
        self.comboBox.setObjectName("comboBox")
        self.gridLayout_2.addWidget(self.comboBox, 0, 2, 1, 1)
        self.labelFiltro = QLabel(self.gridLayoutWidget_4)
        self.labelFiltro.setText("Filtro")
        self.labelFiltro.setFont(QFont("Arial", int(9 * factor)))
        sizePolicy = QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.labelFiltro.sizePolicy().hasHeightForWidth())
        self.labelFiltro.setSizePolicy(sizePolicy)
        self.labelFiltro.setObjectName("labelfiltro")
        self.gridLayout_2.addWidget(self.labelFiltro, 0, 0, 1, 1)
        self.UpdateValidar = QPushButton(self.gridLayoutWidget_4)
        self.UpdateValidar.setObjectName("pushButton")
        self.UpdateValidar.setFont(QFont("Arial", int(9 * factor)))
        self.UpdateValidar.clicked.connect(self.UpdateErrorsEncuesta)
        self.UpdateValidar.setEnabled(False)
        self.gridLayout_2.addWidget(self.UpdateValidar, 3, 0, 1, 3)
        self.view = QTableView(self.gridLayoutWidget_4)
        self.view.setObjectName("tableView")
        self.gridLayout_2.addWidget(self.view, 2, 0, 1, 3)
        self.layout_errores_2.addLayout(self.gridLayout_2, 0, 0, 1, 1)
        self.errores_comentarios_2.addTab(self.tab_errores_2, "")
        self.tab_comentarios_2 = QWidget()
        self.tab_comentarios_2.setObjectName("tab_comentarios_2")
        self.gridLayoutWidget_5 = QWidget(self.tab_comentarios_2)
        self.gridLayoutWidget_5.setGeometry(QRect(10 * factor, 10 * factor, 321 * factor, 71 * factor))
        self.gridLayoutWidget_5.setObjectName("gridLayoutWidget_5")
        self.layout_comentarios_2 = QGridLayout(self.gridLayoutWidget_5)
        self.layout_comentarios_2.setContentsMargins(0, 0, 0, 0)
        self.layout_comentarios_2.setSpacing(2)
        self.layout_comentarios_2.setObjectName("layout_comentarios_2")
        self.groupBox_4 = QGroupBox(self.gridLayoutWidget_5)
        self.groupBox_4.setObjectName("groupBox_4")
        self.verticalLayoutWidget_2 = QWidget(self.groupBox_4)
        self.verticalLayoutWidget_2.setGeometry(QRect(60 * factor, 30 * factor, 211 * factor, 31 * factor))
        self.verticalLayoutWidget_2.setObjectName("verticalLayoutWidget_2")
        self.verticalLayout_4 = QVBoxLayout(self.verticalLayoutWidget_2)
        self.verticalLayout_4.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_4.setObjectName("verticalLayout_4")
        self.agregar_comentario_2 = QPushButton(self.verticalLayoutWidget_2)
        self.agregar_comentario_2.setObjectName("agregar_comentario_2")
        self.agregar_comentario_2.clicked.connect(self.guardar_reporte)
        self.verticalLayout_4.addWidget(self.agregar_comentario_2)
        self.layout_comentarios_2.addWidget(self.groupBox_4, 0, 1, 1, 1)
        self.errores_comentarios_2.addTab(self.tab_comentarios_2, "")
        self.errores_comentarios.setCurrentIndex(0)
        self.errores_comentarios_2.setCurrentIndex(0)
        self.gridLayoutWidget_7 = QWidget(self.tab_comentarios_2)
        self.gridLayoutWidget_7.setGeometry(QRect(10 * factor, 90 * factor, 321 * factor, 71 * factor))
        self.gridLayoutWidget_7.setObjectName("gridLayoutWidget_7")
        self.layout_segundo_hogar = QGridLayout(self.gridLayoutWidget_7)
        self.layout_segundo_hogar.setContentsMargins(0, 0, 0, 0)
        self.layout_segundo_hogar.setSpacing(2)
        self.layout_segundo_hogar.setObjectName("layout_segundo_hogar")
        self.groupbox_segundo_hogar = QGroupBox(self.gridLayoutWidget_7)
        self.groupbox_segundo_hogar.setObjectName("groupbox_segundo_hogar")
        self.verticalLayoutWidget_9 = QWidget(self.groupbox_segundo_hogar)
        self.verticalLayoutWidget_9.setGeometry(QRect(60 * factor, 30 * factor, 211 * factor, 31 * factor))
        self.verticalLayoutWidget_9.setObjectName("verticalLayoutWidget_9")
        self.vertical_layout_segundo_hogar = QVBoxLayout(self.verticalLayoutWidget_9)
        self.vertical_layout_segundo_hogar.setContentsMargins(0, 0, 0, 0)
        self.vertical_layout_segundo_hogar.setObjectName("vertical_layout_segundo_hogar")
        self.boton_segundo_hogar = QPushButton(self.verticalLayoutWidget_9)
        self.boton_segundo_hogar.setObjectName("boton_segundo_hogar")
        self.vertical_layout_segundo_hogar.addWidget(self.boton_segundo_hogar)
        self.layout_segundo_hogar.addWidget(self.groupbox_segundo_hogar, 0, 0, 1, 1)
        self.gridLayoutWidget_11 = QWidget(self.tab_comentarios_2)
        self.gridLayoutWidget_11.setGeometry(QRect(10 * factor, 170 * factor, 321 * factor, 71 * factor))
        self.gridLayoutWidget_11.setObjectName("gridLayoutWidget_11")
        self.layout_reemplazar = QGridLayout(self.gridLayoutWidget_11)
        self.layout_reemplazar.setContentsMargins(0, 0, 0, 0)
        self.layout_reemplazar.setSpacing(2)
        self.layout_reemplazar.setObjectName("layout_reemplazar")
        self.groupbox_reemplazar = QGroupBox(self.gridLayoutWidget_11)
        self.groupbox_reemplazar.setObjectName("groupbox_reemplazar")
        self.verticalLayoutWidget_11 = QWidget(self.groupbox_reemplazar)
        self.verticalLayoutWidget_11.setGeometry(QRect(60 * factor, 30 * factor, 211 * factor, 31 * factor))
        self.verticalLayoutWidget_11.setObjectName("verticalLayoutWidget_11")
        self.vertical_layout_reemplazar = QVBoxLayout(self.verticalLayoutWidget_11)
        self.vertical_layout_reemplazar.setContentsMargins(0, 0, 0, 0)
        self.vertical_layout_reemplazar.setObjectName("vertical_layout_reemplazar")
        self.boton_reemplazar = QPushButton(self.verticalLayoutWidget_11)
        self.boton_reemplazar.setObjectName("boton_reemplazar")
        self.vertical_layout_reemplazar.addWidget(self.boton_reemplazar)
        self.layout_reemplazar.addWidget(self.groupbox_reemplazar, 0, 1, 1, 1)
        self.boton_segundo_hogar.clicked.connect(self.rellena_segundo_hogar)
        self.boton_segundo_hogar.setEnabled(False)
        self.boton_reemplazar.clicked.connect(self.reemplaza_original_control)

        ########################################################################
        # Resto
        ########################################################################

        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QMenuBar(MainWindow)
        self.menubar.setGeometry(QRect(0 * factor, 0 * factor, 1249 * factor, 21 * factor))
        self.menubar.setObjectName("menubar")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QStatusBar(MainWindow)
        self.statusbar.showMessage("Version 2.1 - 8 de Octubre de 2020")
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)
        self.retranslateUi(MainWindow)
        QMetaObject.connectSlotsByName(MainWindow)

#########################################################################
# Clase que contiene funciones utilizadas en interfaz gráfica de usuario
#########################################################################


class MainWindow(QMainWindow, Ui_MainWindow):

    def __init__(self, parent=None):
        super(MainWindow, self).__init__(parent)
        self.setupUi(self)
        self.setAttribute(Qt.WA_DeleteOnClose)

        # CMD Icono
        self.setWindowIcon(QIcon(":/images/cmd.ico"))

        # Inicia login
        self.login = Ui_Login(self)
        self.login.show()
        #self.show()

    ########################################################################
    # Función para reajustar tamaño del widget
    ########################################################################

    def resizeEvent(self, event):
        if (event.type() == QEvent.Resize):
            # print(self.width(), self.height())

            self.view.setGeometry(QRect(1200 * factor, 510 * factor, 350 * factor + (self.width() - 1600 * factor), 430 * factor + (self.height() - 1000 * factor)))
            self.UpdateValidar.setGeometry(QRect(1470 * factor + (self.width() - 1600 * factor), 946 * factor + (self.height() - 1000 * factor), 75 * factor, 30 * factor))

            self.listError.setGeometry(QRect(1200 * factor, 120 * factor, 350 * factor + (self.width() - 1600 * factor), 250 * factor))
            self.UpdateErrors.setGeometry(QRect(1475 * factor + (self.width() - 1600 * factor), 375 * factor, 75 * factor, 30 * factor))

            self.errores_comentarios.setGeometry(QRect(1200 * factor, 100 * factor, 347 * factor + (self.width() - 1600 * factor), 301 * factor))
            self.gridLayoutWidget_3.setGeometry(QRect(0, 0, 339 * factor + (self.width() - 1600 * factor), 271 * factor))
            self.gridLayoutWidget_2.setGeometry(QRect(0, 0, 339 * factor + (self.width() - 1600 * factor), 271 * factor))

            self.errores_comentarios_2.setGeometry(QRect(1200 * factor, 450 * factor, 347 * factor + (self.width() - 1600 * factor), 521 * factor))
            self.gridLayoutWidget_4.setGeometry(QRect(0, 0, 339 * factor + (self.width() - 1600 * factor), 491 * factor))
            self.gridLayoutWidget_5.setGeometry(QRect(10 * factor, 10 * factor, 321 * factor + (self.width() - 1600 * factor), 71 * factor))
            self.verticalLayoutWidget_2.setGeometry(QRect(60 * factor, 30 * factor, 211 * factor + (self.width() - 1600 * factor), 31 * factor))
            self.verticalLayoutWidget_9.setGeometry(QRect(60 * factor, 30 * factor, 211 * factor + (self.width() - 1600 * factor), 31 * factor))
            self.verticalLayoutWidget_11.setGeometry(QRect(60 * factor, 30 * factor, 211 * factor + (self.width() - 1600 * factor), 31 * factor))
            self.gridLayoutWidget_7.setGeometry(QRect(10 * factor, 90 * factor, 321 * factor + (self.width() - 1600 * factor), 71 * factor))
            self.gridLayoutWidget_11.setGeometry(QRect(10 * factor, 170 * factor, 321 * factor + (self.width() - 1600 * factor), 71 * factor))
            if self.username == 'admin':
                self.gridLayoutWidget_8.setGeometry(QRect(10 * factor, 250 * factor, 321 * factor + (self.width() - 1600 * factor), 181 * factor))
                self.gridLayoutWidget_9.setGeometry(QRect(60 * factor, 30 * factor, 211 * factor + (self.width() - 1600 * factor), 71 * factor))
    
    ########################################################################
    # Traducción Botones
    ########################################################################

    def retranslateUi(self, MainWindow):
        _translate = QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "Validador EOD - CMD"))
        self.cargar.setText(_translate("MainWindow", "Buscar"))
        self.UpdateValidar.setText(_translate("MainWindow", "Actualizar"))
        self.UpdateErrors.setText(_translate("MainWindow", "Actualizar"))
        self.guardar.setText(_translate("MainWindow", "Guardar"))
        self.groupBox.setTitle(_translate("MainWindow", "Información de Encuesta"))
        self.label_4.setText(_translate("MainWindow", "Teléfono:"))
        self.label_2.setText(_translate("MainWindow", "Coordinador:"))
        self.label_3.setText(_translate("MainWindow", "Hogar:"))
        self.label_6.setText(_translate("MainWindow", "Encuestador:"))
        self.label_8.setText(_translate("MainWindow", "N° Personas Hogar:"))
        self.label_1.setText(_translate("MainWindow", "Segmento:"))
        self.errores_comentarios.setTabText(self.errores_comentarios.indexOf(self.tab_errores), _translate("MainWindow", "Errores en Encuesta"))
        self.agregar_comentario.setText(_translate("MainWindow", "Agregar Comentario"))
        self.errores_comentarios.setTabText(self.errores_comentarios.indexOf(self.tab_comentarios), _translate("MainWindow", "Comentarios"))
        self.labelFiltro.setText(_translate("MainWindow", "Filtro:"))
        self.UpdateValidar.setText(_translate("MainWindow", "Actualizar"))
        self.errores_comentarios_2.setTabText(self.errores_comentarios_2.indexOf(self.tab_errores_2), _translate("MainWindow", "Encuestas por Validar"))
        self.groupBox_4.setTitle(_translate("MainWindow", "Descargar reporte de estado de encuestas"))
        self.agregar_comentario_2.setText(_translate("MainWindow", "Descargar"))
        self.errores_comentarios_2.setTabText(self.errores_comentarios_2.indexOf(self.tab_comentarios_2), _translate("MainWindow", "Herramientas"))
        self.groupbox_segundo_hogar.setTitle(_translate("MainWindow", "Rellenar Identificación de Segundo Hogar"))
        self.boton_segundo_hogar.setText(_translate("MainWindow", "Rellenar"))
        self.groupbox_reemplazar.setTitle(_translate("MainWindow", "Reemplazar Encuesta Original por Control"))
        self.boton_reemplazar.setText(_translate("MainWindow", "Reemplazar"))

    #########################################################################
    # Función que rellena información de identificación para segundos hogares
    #########################################################################

    def rellena_segundo_hogar(self):
        self.rellenar_segundohogar = Ui_SegHog(self)
        self.rellenar_segundohogar.show()

    #########################################################################
    # Función para actualizar estado de input revisar codificación
    #########################################################################

    def reemplaza_original_control(self):
        self.reemplazar_controloriginal = Ui_ControlOriginal(self)
        self.reemplazar_controloriginal.show()

    ############################################################################
    # Habilita botones al conectar
    ############################################################################

    def habilita_botones(self):
        self.UpdateValidar.setEnabled(True)
        self.UpdateErrors.setEnabled(True)
        self.guardar.setEnabled(True)
        self.cargar.setEnabled(True)
        self.agregar_comentario.setEnabled(True)

    ############################################################################
    # Guardar
    ############################################################################

    def guardar_reporte(self):
        # Seleccionamos variables
        query = f'SELECT * FROM {base_sql}.{table_sql}'
        # Descargamos la base para el reporte
        self.reporte, result = FunVal.DescargaSql(text_user, pass_sql, base_sql, query)

        # Generamos el cuadro de diálogo para guardar archivos
        path = App().saveFileDialog()

        global path_user
        path_user = ""
        # print(path)

        # Creamos la ruta correcta sin duplicar extensión del archivo
        try:
            path_user = path
            if path.split(".")[-1] != "xlsx":
                path_user = path + ".xlsx"
            # print(path_user)
        except Exception as exception:
            advertencia_directorio_reporte = QMessageBox()
            advertencia_directorio_reporte.setIcon(QMessageBox.Critical)
            advertencia_directorio_reporte.setText("Debe seleccionar un directorio para descargar el reporte seleccionado")
            advertencia_directorio_reporte.setWindowTitle("Error de directorio")

        if path_user is not None:
            self.reporte = self.reporte.sort_values(by=['act']).drop_duplicates(subset=['interview__key', 'orden'], keep='last')
            self.df_Errores = External(self).UpdListError2(self.reporte, rev_cod=self.RevCod1, key=None)
            # df_Errores.to_excel("C:\\Users\\Javie\\Downloads\\df_Errores.xlsx")
            # self.df.to_csv("C:\\Users\\usuario\\Downloads\\df.csv")

            # df_Errores = pd.read_csv("C:\\Users\\usuario\\Downloads\\df_Errores.csv")
            # df = pd.read_csv("C:\\Users\\usuario\\Downloads\\df.csv")
            self.le = self.df_Errores['interview__key'].drop_duplicates()

            self.reporte = self.reporte.merge(self.le, on='interview__key', indicator='exists', how='outer')
            self.reporte = self.reporte.loc[self.reporte.exists != "right_only", :]
            # self.reporte.loc[(self.reporte._merge!="both"),['estado']] =   0 # Encuestas que no pegaron, no tienen errores entonces: 1) Silenciar mantener historial cambios, 2) Descomentar para reinicar
            self.reporte.loc[(self.reporte.exists == "both") & (self.reporte.estado == 0), ['estado']] = 1  # Encuestas que  pegaron,  tienen errores
            self.reporte.loc[(self.reporte.exists == "both") & (self.reporte.estado == 1), ['estado']] = 1  # Encuestas que  pegaron,  tienen errores
            self.reporte.loc[(self.reporte.exists == "both") & (self.reporte.estado == 2), ['estado']] = 1  # Encuestas que  pegaron,  tienen errores
            self.reporte.loc[(self.reporte.exists == "both") & (self.reporte.estado == 3), ['estado']] = 3  # Encuestas que  pegaron forzadas,  dejarlas forzadas
            self.reporte = self.reporte.drop(['exists'], axis=1)
            var = ['interview__key', 'tipo_muestra', 'cuarto', 'estrato', 'comuna', 'segmento', 'hogar', 'encuesta', 'gse', 'q_resp', 'nombre', 'orden', 'numper', 'pcoh', 'sexo', 'edad', 'coordinador', 'encuestador', 'act', 'estado']
            self.reporte = self.reporte.loc[:, var]
            # self.reporte.drop(columns = ['interview__key'], axis = 1, inplace = True)
            self.reporte.loc[self.reporte['estado'] == 0, 'estado'] = "Sin validar (correcta)"
            self.reporte.loc[self.reporte['estado'] == 1, 'estado'] = "Sin validar (con errores)"
            self.reporte.loc[self.reporte['estado'] == 2, 'estado'] = "Validada"
            self.reporte.loc[self.reporte['estado'] == 3, 'estado'] = "Validación forzada"
            self.reporte.to_excel(os.path.join(path), index=False)

    def sql_to_excel(self):
        # Seleccionamos variables
        query = f'SELECT * FROM {base_sql}.{table_sql}'
        # Descargamos la base para el reporte
        self.reporte, result = FunVal.DescargaSql(text_user, pass_sql, base_sql, query)

        # Generamos el cuadro de diálogo para guardar archivos
        path = App().saveFileDialog()

        global path_user
        path_user = ""
        # print(path)

        # Creamos la ruta correcta sin duplicar extensión del archivo
        try:
            path_user = path
            if path.split(".")[-1] != "xlsx":
                path_user = path + ".xlsx"
            # print(path_user)
        except Exception as exception:
            advertencia_directorio_reporte = QMessageBox()
            advertencia_directorio_reporte.setIcon(QMessageBox.Critical)
            advertencia_directorio_reporte.setText("Debe seleccionar un directorio para descargar el reporte seleccionado")
            advertencia_directorio_reporte.setWindowTitle("Error de directorio")

        if path_user is not None:
            self.reporte = self.reporte.sort_values(by=['act']).drop_duplicates(subset=['interview__key', 'orden'], keep='last')
            self.df_Errores = External(self).UpdListError2(self.reporte, rev_cod=self.RevCod1, key=None)
            # df_Errores.to_excel("C:\\Users\\Javie\\Downloads\\df_Errores.xlsx")
            # self.df.to_csv("C:\\Users\\usuario\\Downloads\\df.csv")

            # df_Errores = pd.read_csv("C:\\Users\\usuario\\Downloads\\df_Errores.csv")
            # df = pd.read_csv("C:\\Users\\usuario\\Downloads\\df.csv")
            self.le = self.df_Errores['interview__key'].drop_duplicates()
            self.reporte = self.reporte.merge(self.le, on='interview__key', indicator='exists', how='outer')
            self.reporte = self.reporte.loc[self.reporte.exists != "right_only", :]
            # self.reporte.loc[(self.reporte._merge!="both"),['estado']] =   0 # Encuestas que no pegaron, no tienen errores entonces: 1) Silenciar mantener historial cambios, 2) Descomentar para reinicar
            self.reporte.loc[(self.reporte.exists == "both") & (self.reporte.estado == 0), ['estado']] = 1  # Encuestas que  pegaron,  tienen errores
            self.reporte.loc[(self.reporte.exists == "both") & (self.reporte.estado == 1), ['estado']] = 1  # Encuestas que  pegaron,  tienen errores
            self.reporte.loc[(self.reporte.exists == "both") & (self.reporte.estado == 2), ['estado']] = 1  # Encuestas que  pegaron,  tienen errores
            self.reporte.loc[(self.reporte.exists == "both") & (self.reporte.estado == 3), ['estado']] = 3  # Encuestas que  pegaron forzadas,  dejarlas forzadas
            self.reporte = self.reporte.drop(['exists'], axis=1)
            # self.reporte.drop(columns = ['interview__key'], axis = 1, inplace = True)
            self.reporte.loc[self.reporte['estado'] == 0, 'estado'] = "Sin validar (correcta)"
            self.reporte.loc[self.reporte['estado'] == 1, 'estado'] = "Sin validar (con errores)"
            self.reporte.loc[self.reporte['estado'] == 2, 'estado'] = "Validada"
            self.reporte.loc[self.reporte['estado'] == 3, 'estado'] = "Validación forzada"
            self.reporte.to_excel(os.path.join(path), index=False)

    def sql_to_stata(self):

        # Seleccionamos variables
        query = f'SELECT * FROM {base_sql}.{table_sql}'
        # Descargamos la base para el reporte
        self.reporte, result = FunVal.DescargaSql(text_user, pass_sql, base_sql, query)

        # Generamos el cuadro de diálogo para guardar archivos
        path = App().saveFileDialog()

        global path_user
        path_user = ""
        # print(path)

        # Creamos la ruta correcta sin duplicar extensión del archivo
        try:
            path_user = path
            if path.split(".")[-1] != "dta":
                path_user = path + ".dta"
            # print(path_user)
        except Exception as exception:
            advertencia_directorio_reporte = QMessageBox()
            advertencia_directorio_reporte.setIcon(QMessageBox.Critical)
            advertencia_directorio_reporte.setText("Debe seleccionar un directorio para descargar el reporte seleccionado")
            advertencia_directorio_reporte.setWindowTitle("Error de directorio")

        if path_user is not None:
            self.reporte = self.reporte.sort_values(by=['act']).drop_duplicates(subset=['interview__key', 'orden'], keep='last')
            self.df_Errores = External(self).UpdListError2(self.reporte, rev_cod=self.RevCod1, key=None)
            # df_Errores.to_excel("C:\\Users\\Javie\\Downloads\\df_Errores.xlsx")
            # self.df.to_csv("C:\\Users\\usuario\\Downloads\\df.csv")

            # df_Errores = pd.read_csv("C:\\Users\\usuario\\Downloads\\df_Errores.csv")
            # df = pd.read_csv("C:\\Users\\usuario\\Downloads\\df.csv")
            self.le = self.df_Errores['interview__key'].drop_duplicates()

            self.reporte = self.reporte.merge(self.le, on='interview__key', indicator='exists', how='outer')
            self.reporte = self.reporte.loc[self.reporte.exists != "right_only", :]
            # self.reporte.loc[(self.reporte._merge!="both"),['estado']] =   0 # Encuestas que no pegaron, no tienen errores entonces: 1) Silenciar mantener historial cambios, 2) Descomentar para reinicar
            self.reporte.loc[(self.reporte.exists == "both") & (self.reporte.estado == 0), ['estado']] = 1  # Encuestas que  pegaron,  tienen errores
            self.reporte.loc[(self.reporte.exists == "both") & (self.reporte.estado == 1), ['estado']] = 1  # Encuestas que  pegaron,  tienen errores
            self.reporte.loc[(self.reporte.exists == "both") & (self.reporte.estado == 2), ['estado']] = 1  # Encuestas que  pegaron,  tienen errores
            self.reporte.loc[(self.reporte.exists == "both") & (self.reporte.estado == 3), ['estado']] = 3  # Encuestas que  pegaron forzadas,  dejarlas forzadas
            self.reporte = self.reporte.drop(['exists'], axis=1)
            # self.reporte.drop(columns = ['interview__key'], axis = 1, inplace = True)
            self.reporte.loc[self.reporte['estado'] == 0, 'estado'] = "Sin validar (correcta)"
            self.reporte.loc[self.reporte['estado'] == 1, 'estado'] = "Sin validar (con errores)"
            self.reporte.loc[self.reporte['estado'] == 2, 'estado'] = "Validada"
            self.reporte.loc[self.reporte['estado'] == 3, 'estado'] = "Validación forzada"
            self.reporte.to_stata(os.path.join(path), write_index=False, version=118)


    ############################################################################
    # Funciones para cambio de hoja
    ############################################################################

    def cambio_hoja(self, i):
        if i == 0:
            # Cambia tabla
            self.tableWidget = QTableWidget(self.gridLayoutWidget_6)
            sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            sizePolicy.setHorizontalStretch(0)
            sizePolicy.setVerticalStretch(0)
            sizePolicy.setHeightForWidth(self.tableWidget.sizePolicy().hasHeightForWidth())
            self.tableWidget.setSizePolicy(sizePolicy)
            self.tableWidget.setMinimumSize(QSize(0, 250 * factor))
            self.tableWidget.setRowCount(0)  # Numero de columna debería quedar en función del numero de personas
            self.tableWidget.setColumnCount(20)  # Varia por hoja
            self.tableWidget.setObjectName("tabla")  # nombre de la tabla
            self.tableWidget.setFont(QFont("Arial", int(9)))
            self.tableWidget.setColumnWidth(0, 114 * factor)
            self.tableWidget.setColumnWidth(1, 57 * factor)
            self.tableWidget.setColumnWidth(2, 39 * factor)
            self.tableWidget.setColumnWidth(3, 39 * factor)
            self.tableWidget.setColumnWidth(4, 67 * factor)
            self.tableWidget.setColumnWidth(5, 57 * factor)
            self.tableWidget.setColumnWidth(6, 95 * factor)
            self.tableWidget.setColumnWidth(7, 48 * factor)
            self.tableWidget.setColumnWidth(8, 48 * factor)
            self.tableWidget.setColumnWidth(9, 39 * factor)
            self.tableWidget.setColumnWidth(10, 39 * factor)
            self.tableWidget.setColumnWidth(11, 39 * factor)
            self.tableWidget.setColumnWidth(12, 39 * factor)
            self.tableWidget.setColumnWidth(13, 39 * factor)
            self.tableWidget.setColumnWidth(14, 48 * factor)
            self.tableWidget.setColumnWidth(15, 57 * factor)
            self.tableWidget.setColumnWidth(16, 95 * factor)
            self.tableWidget.setColumnWidth(17, 57 * factor)
            self.tableWidget.setColumnWidth(18, 67 * factor)
            self.tableWidget.setColumnWidth(19, 48 * factor)
            self.tableWidget.cellChanged.connect(self.onCellChanged)
            self.tableWidget.horizontalHeader().setFont(QFont("Arial", int(9), QFont.Bold))
            self.tableWidget.verticalHeader().setVisible(False)
            self.grid_imagenes.addWidget(self.tableWidget, 1, 0, 1, 1)
            self.tableWidget.show()

            if self.df2.empty is False:
                self.df3 = self.df2.loc[:, [
                    'nombre', 'pcoh', 'sexo', 'edad', 'p4a', 'p4b', 'p4b_esp',
                    'p4c', 'p4d', 'p5a', 'p5b_c', 'p5b', 'p5c_c', 'p5c',
                    'p6a', 'p6a1', 'p6c', 'p6d', 'p6d_esp', 'p6d1']]

                self.write_df_to_qtable(self.df3, self.tableWidget, self.df_Errores)
            else:
                pass

        if i == 1:
            # Cambia tabla
            self.tableWidget = QTableWidget(self.gridLayoutWidget_6)
            sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            sizePolicy.setHorizontalStretch(0)
            sizePolicy.setVerticalStretch(0)
            sizePolicy.setHeightForWidth(self.tableWidget.sizePolicy().hasHeightForWidth())
            self.tableWidget.setSizePolicy(sizePolicy)
            self.tableWidget.setMinimumSize(QSize(0, 250 * factor))
            self.tableWidget.setRowCount(0)  # Numero de columna debería quedar en función del numero de personas
            self.tableWidget.setColumnCount(17)  # Varia por hoja
            self.tableWidget.setObjectName("tabla")  # nombre de la tabla
            self.tableWidget.setFont(QFont("Arial", int(9)))
            self.tableWidget.setColumnWidth(0, 114 * factor)
            self.tableWidget.setColumnWidth(1, 48 * factor)
            self.tableWidget.setColumnWidth(2, 67 * factor)
            self.tableWidget.setColumnWidth(3, 67 * factor)
            self.tableWidget.setColumnWidth(4, 48 * factor)
            self.tableWidget.setColumnWidth(5, 48 * factor)
            self.tableWidget.setColumnWidth(6, 95 * factor)
            self.tableWidget.setColumnWidth(7, 35 * factor)
            self.tableWidget.setColumnWidth(8, 35 * factor)
            self.tableWidget.setColumnWidth(9, 35 * factor)
            self.tableWidget.setColumnWidth(10, 105 * factor)
            self.tableWidget.setColumnWidth(11, 35 * factor)
            self.tableWidget.setColumnWidth(12, 35 * factor)
            self.tableWidget.setColumnWidth(13, 35 * factor)
            self.tableWidget.setColumnWidth(14, 105 * factor)
            self.tableWidget.setColumnWidth(15, 90 * factor)
            self.tableWidget.setColumnWidth(16, 90 * factor)
            self.tableWidget.cellChanged.connect(self.onCellChanged)
            self.tableWidget.horizontalHeader().setFont(QFont("Arial", int(9), QFont.Bold))
            self.tableWidget.verticalHeader().setVisible(False)
            self.grid_imagenes.addWidget(self.tableWidget, 1, 0, 1, 1)
            self.tableWidget.show()

            if self.df2.empty is False:
                self.df3 = self.df2.loc[:, ['nombre', 'p6e0', 'p6e', 'p6f', 'p6g', 'p6g1', 'p6g1_esp', 'p6h', 'p6h1', 'p6h2', 'p6h_esp', 'p6i', 'p6i1', 'p6i2', 'p6i_esp', 'p7', 'oficio']]

                # Takes a df and writes it to a qtable provided. df headers become qtable headers

                self.write_df_to_qtable(self.df3, self.tableWidget, self.df_Errores)
            else:
                pass

        if i == 2:
            self.tableWidget = QTableWidget(self.gridLayoutWidget_6)
            sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            sizePolicy.setHorizontalStretch(0)
            sizePolicy.setVerticalStretch(0)
            sizePolicy.setHeightForWidth(self.tableWidget.sizePolicy().hasHeightForWidth())
            self.tableWidget.setSizePolicy(sizePolicy)
            self.tableWidget.setMinimumSize(QSize(0, 250 * factor))
            self.tableWidget.setRowCount(0)  # Numero de columna debería quedar en función del numero de personas
            self.tableWidget.setColumnCount(15)  # Varia por hoja
            self.tableWidget.setObjectName("tabla")  # nombre de la tabla
            self.tableWidget.setFont(QFont("Arial", int(9)))
            self.tableWidget.setColumnWidth(0, 114 * factor)
            self.tableWidget.setColumnWidth(1, 105 * factor)
            self.tableWidget.setColumnWidth(2, 105 * factor)
            self.tableWidget.setColumnWidth(3, 114 * factor)
            self.tableWidget.setColumnWidth(4, 67 * factor)
            self.tableWidget.setColumnWidth(5, 48 * factor)
            self.tableWidget.setColumnWidth(6, 48 * factor)
            self.tableWidget.setColumnWidth(7, 48 * factor)
            self.tableWidget.setColumnWidth(8, 48 * factor)
            self.tableWidget.setColumnWidth(9, 48 * factor)
            self.tableWidget.setColumnWidth(10, 57 * factor)
            self.tableWidget.setColumnWidth(11, 86 * factor)
            self.tableWidget.setColumnWidth(12, 125 * factor)
            self.tableWidget.setColumnWidth(13, 57 * factor)
            self.tableWidget.setColumnWidth(14, 57 * factor)
            self.tableWidget.cellChanged.connect(self.onCellChanged)
            self.tableWidget.horizontalHeader().setFont(QFont("Arial", int(9), QFont.Bold))
            self.tableWidget.verticalHeader().setVisible(False)
            self.grid_imagenes.addWidget(self.tableWidget, 1, 0, 1, 1)
            self.tableWidget.show()

            if self.df2.empty is False:
                self.df3 = self.df2.loc[:, ['nombre', 'posocup', 'ntrab', 'p9', 'p9_cod1', 'horas', 'dias', 'p10a1', 'p10a2', 'p10b1', 'varh', 'varh2', 'p11a', 'p11a_tab', 'p11a_cod']]

                # Takes a df and writes it to a qtable provided. df headers become qtable headers

                self.write_df_to_qtable(self.df3, self.tableWidget, self.df_Errores)
            else:
                pass

        if i == 3:
            self.tableWidget = QTableWidget(self.gridLayoutWidget_6)
            sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            sizePolicy.setHorizontalStretch(0)
            sizePolicy.setVerticalStretch(0)
            sizePolicy.setHeightForWidth(self.tableWidget.sizePolicy().hasHeightForWidth())
            self.tableWidget.setSizePolicy(sizePolicy)
            self.tableWidget.setMinimumSize(QSize(0, 250 * factor))
            self.tableWidget.setRowCount(0)  # Numero de columna debería quedar en función del numero de personas
            self.tableWidget.setColumnCount(14)  # Varia por hoja
            self.tableWidget.setObjectName("tabla")  # nombre de la tabla
            self.tableWidget.setFont(QFont("Arial", int(9)))
            self.tableWidget.setColumnWidth(0, 114 * factor)
            self.tableWidget.setColumnWidth(1, 114 * factor)
            self.tableWidget.setColumnWidth(2, 57 * factor)
            self.tableWidget.setColumnWidth(3, 57 * factor)
            self.tableWidget.setColumnWidth(4, 70 * factor)
            self.tableWidget.setColumnWidth(5, 70 * factor)
            self.tableWidget.setColumnWidth(6, 125 * factor)
            self.tableWidget.setColumnWidth(7, 67 * factor)
            self.tableWidget.setColumnWidth(8, 67 * factor)
            self.tableWidget.setColumnWidth(9, 48 * factor)
            self.tableWidget.setColumnWidth(10, 114 * factor)
            self.tableWidget.setColumnWidth(11, 77 * factor)
            self.tableWidget.setColumnWidth(12, 77 * factor)
            self.tableWidget.setColumnWidth(13, 77 * factor)
            self.tableWidget.cellChanged.connect(self.onCellChanged)
            self.tableWidget.horizontalHeader().setFont(QFont("Arial", int(9), QFont.Bold))
            self.tableWidget.verticalHeader().setVisible(False)
            self.grid_imagenes.addWidget(self.tableWidget, 1, 0, 1, 1)
            self.tableWidget.show()

            if self.df2.empty is False:
                # Javier - 06-06-2020
                self.df3 = self.df2.loc[:, [
                    'nombre', 'p11b_esp', 'p11b_tab', 'p11b_cod', 'esfuer', 'p11d', 'ntrab2', 'acttrabm', 'acttraba', 'contrato',
                    'relcon', 'varw', 'varw2', 'varw4']]

                # Takes a df and writes it to a qtable provided. df headers become qtable headers

                self.write_df_to_qtable(self.df3, self.tableWidget, self.df_Errores)
            else:
                pass

        if i == 4:
            self.tableWidget = QTableWidget(self.gridLayoutWidget_6)
            sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            sizePolicy.setHorizontalStretch(0)
            sizePolicy.setVerticalStretch(0)
            sizePolicy.setHeightForWidth(self.tableWidget.sizePolicy().hasHeightForWidth())
            self.tableWidget.setSizePolicy(sizePolicy)
            self.tableWidget.setMinimumSize(QSize(0, 250 * factor))
            self.tableWidget.setRowCount(0)  # Numero de columna debería quedar en función del numero de personas
            self.tableWidget.setColumnCount(9)  # Varia por hoja
            self.tableWidget.setObjectName("tabla")  # nombre de la tabla
            self.tableWidget.setFont(QFont("Arial", int(9)))
            self.tableWidget.setColumnWidth(0, 114 * factor)
            self.tableWidget.setColumnWidth(1, 134 * factor)
            self.tableWidget.setColumnWidth(2, 124 * factor)
            self.tableWidget.setColumnWidth(3, 125 * factor)
            self.tableWidget.setColumnWidth(4, 77 * factor)
            self.tableWidget.setColumnWidth(5, 77 * factor)
            self.tableWidget.setColumnWidth(6, 172 * factor)
            self.tableWidget.setColumnWidth(7, 152 * factor)
            self.tableWidget.setColumnWidth(8, 152 * factor)
            self.tableWidget.cellChanged.connect(self.onCellChanged)
            self.tableWidget.horizontalHeader().setFont(QFont("Arial", int(9), QFont.Bold))
            self.tableWidget.verticalHeader().setVisible(False)
            self.grid_imagenes.addWidget(self.tableWidget, 1, 0, 1, 1)
            self.tableWidget.show()

            if self.df2.empty is False:
                self.df3 = self.df2.loc[:, ['nombre', 'p18a', 'p18c', 'p18d', 'p18e', 'p18f', 'p18g', 'p18h', 'p18i']]
                self.write_df_to_qtable(self.df3, self.tableWidget, self.df_Errores)
            else:
                pass

        if i == 5:
            self.tableWidget = QTableWidget(self.gridLayoutWidget_6)
            sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            sizePolicy.setHorizontalStretch(0)
            sizePolicy.setVerticalStretch(0)
            sizePolicy.setHeightForWidth(self.tableWidget.sizePolicy().hasHeightForWidth())
            self.tableWidget.setSizePolicy(sizePolicy)
            self.tableWidget.setMinimumSize(QSize(0, 250 * factor))
            self.tableWidget.setRowCount(0)  # Numero de columna debería quedar en función del numero de personas
            self.tableWidget.setColumnCount(14)  # Varia por hoja
            self.tableWidget.setObjectName("tabla")  # nombre de la tabla
            self.tableWidget.setFont(QFont("Arial", int(9)))
            self.tableWidget.setColumnWidth(0, 114 * factor)
            self.tableWidget.setColumnWidth(1, 48 * factor)
            self.tableWidget.setColumnWidth(2, 39 * factor)
            self.tableWidget.setColumnWidth(3, 39 * factor)
            self.tableWidget.setColumnWidth(4, 39 * factor)
            self.tableWidget.setColumnWidth(5, 39 * factor)
            self.tableWidget.setColumnWidth(6, 39 * factor)
            self.tableWidget.setColumnWidth(7, 39 * factor)
            self.tableWidget.setColumnWidth(8, 39 * factor)
            self.tableWidget.setColumnWidth(9, 95 * factor)
            self.tableWidget.setColumnWidth(10, 47 * factor)
            self.tableWidget.setColumnWidth(11, 191 * factor)
            self.tableWidget.setColumnWidth(12, 180 * factor)
            self.tableWidget.setColumnWidth(13, 180 * factor)

            self.tableWidget.cellChanged.connect(self.onCellChanged)
            self.tableWidget.horizontalHeader().setFont(QFont("Arial", int(9), QFont.Bold))
            self.tableWidget.verticalHeader().setVisible(False)
            self.grid_imagenes.addWidget(self.tableWidget, 1, 0, 1, 1)
            self.tableWidget.show()

            if self.df2.empty is False:
                self.df3 = self.df2.loc[:, ['nombre', 'c1', 'c2a', 'c2b', 'c2c', 'c2d', 'c2e', 'c2f', 'c2g', 'c2_esp', 'c3', 'c4', 'c5', 'c6']]

                # Takes a df and writes it to a qtable provided. df headers become qtable headers

                self.write_df_to_qtable(self.df3, self.tableWidget, self.df_Errores)
            else:
                pass

    ############################################################################
    # Funciones para tabla filtro
    ############################################################################

    @pyqtSlot(int, name='on_view_horizontalHeader_sectionClicked')
    def on_view_horizontalHeader_sectionClicked(self, logicalIndex):

        if logicalIndex > 1:
            self.logicalIndex = logicalIndex  # => Index de la columna en donde se hace click
            self.menuValues = QMenu(self)
            self.signalMapper = QSignalMapper(self)
            self.comboBox.blockSignals(True)  # => Bloquea señal de index de cambio de combo obx
            # self.comboBox.setCurrentIndex(self.logicalIndex) #=> Fija index de combobox para coincida con la columna que se hizo click

            valuesUnique = [
                self.model.item(row, self.logicalIndex).text()
                for row in range(self.model.rowCount())]

            actionAll = QAction("Todes", self)  # => Titulo Lista de valores que se mostraran al hacer click
            actionAll.triggered.connect(self.on_actionAll_triggered)  # => Accion para limpiar filtro
            self.menuValues.addAction(actionAll)   # => Lista de valores que se mostraran al hacer click
            self.menuValues.addSeparator()  # => Formato para Lista que se muestra

            # => Crea objeto de lista que se muestra con accion vinculada a cada opción
            for actionNumber, actionName in enumerate(sorted(list(set(valuesUnique)))):
                action = QAction(actionName, self)
                self.signalMapper.setMapping(action, actionNumber)
                action.triggered.connect(self.signalMapper.map)
                self.menuValues.addAction(action)

            self.signalMapper.mapped.connect(self.on_signalMapper_mapped)

            headerPos = self.view.mapToGlobal(self.horizontalHeader.pos())  # => Posición para lista que se despliega
            posY = headerPos.y() + self.horizontalHeader.height()  # => Posición para lista que se despliega
            posX = headerPos.x() + self.horizontalHeader.sectionPosition(self.logicalIndex) + 30  # => Posición para lista que se despliega

            self.menuValues.setStyleSheet("QMenu { menu-scrollable: 1; }")
            self.menuValues.setMaximumHeight(800 * factor)
            # self.menuValues.setGeometry(QRect(1200 * factor, 510 * factor, 100 * factor, 300 * factor))
            # self.menuValues.setMaximumWidth(100)
            self.menuValues.move(QPoint(posX, posY))
            self.menuValues.exec()  # => Posición para lista que se despliega
            self.comboBox.blockSignals(False)  # => Permite señales nuevamente del combo box

    @pyqtSlot(name='on_actionAll_triggered')
    def on_actionAll_triggered(self):
        filterColumn = self.logicalIndex
        filterString = QRegExp(
            "",
            Qt.CaseInsensitive,
            QRegExp.RegExp)

        self.proxy.setFilterRegExp(filterString)  # => Buscara "" (todos)
        self.proxy.setFilterKeyColumn(filterColumn)  # => En columna que se hiz click
        self.proxy0.setSourceModel(self.proxy)

    @pyqtSlot(int, name='on_signalMapper_mapped')
    def on_signalMapper_mapped(self, i):
        stringAction = self.signalMapper.mapping(i).text()  # => Texto que buscar
        filterColumn = self.logicalIndex  # => Columna donde buscar
        filterString = QRegExp(
            stringAction,
            Qt.CaseSensitive,
            QRegExp.FixedString)

        self.proxy.setFilterRegExp(filterString)
        self.proxy.setFilterKeyColumn(filterColumn)
        self.proxy0.setSourceModel(self.proxy)

    @pyqtSlot(str, name='on_lineEdit_textChanged')
    def on_lineEdit_textChanged(self, text):
        search = QRegExp(
            text,
            Qt.CaseInsensitive,
            QRegExp.RegExp)

        self.proxy0.setSourceModel(self.proxy)
        self.proxy0.setFilterKeyColumn(self.indexcombobox)
        self.proxy0.setFilterRegExp(search)

    @pyqtSlot(name='on_comboBox_currentIndexChanged')
    def on_comboBox_currentIndexChanged(self):

        self.indexcombobox = self.comboBox.currentIndex()
        self.proxy0.setSourceModel(self.proxy)
        self.proxy0.setFilterKeyColumn(self.indexcombobox)
        # print(self.indexcombobox)

    ########################################################################
    # Actualización de SQL y dataframe local a partir de widget
    # Validación o no validación de la encuesta revisada
    ########################################################################

    def GuardarCambios(self):
        self.UpdateErrorsKey()
        self.n_errores = len(self.df_Errores)
        self.ventana_guardar = Ui_Dialog(self)
        self.ventana_guardar.show()

    ########################################################################
    # Actualización de dataframe a partir de widget
    ########################################################################

    @pyqtSlot(int, int)
    def onCellChanged(self, row, column):
        self.i = self.i + 1

        if self.i > self.threshold:

            correcto = 0

            # Obtenemos el nombre de la columna modificada
            item = self.tableWidget.horizontalHeaderItem(column).text()

            # Obtenemos el texto de la celda modificada
            text = self.tableWidget.item(row, column).text()

            # A valores vacios los define como nan -> Esto permite ingresar vacios desde el widget al dataframe
            # print("cell:", text)
            if text == "":

                text = np.nan
                correcto = -1

            else:
                # Evita cambio de formato

                # type(text)
                try:
                    float(text)
                    # print("Es numero")
                    if self.df[item].dtype == object:  # en los dataframe, object es una cadena de string
                        error = "Se esta ingresando un numero en una variable texto. No se efectuaran los cambios."
                        correcto = 0
                    else:
                        correcto = 1
                except Exception as exception:
                    if self.df[item].dtype == object:
                        correcto = 1
                    else:
                        error = "Se esta ingresando un texto en una variable numerica. No se efectuaran los cambios."
                        correcto = 0

            if correcto in [-1, 1]:

                # Reemplazamos en nuestra base de datos que construye el QTableWidget
                self.df3.iat[row, column] = text

                # Obtenemos el índice de la fila modificada
                index = self.df3.index.values.astype(int)[row]

                # Modificamos en la base que contiene a todas las personas asociadas a un interview__key,
                # que contiene además todas las variables de la base
                self.df2.at[index, item] = text

                # Se actualiza la fecha a partir de la modificación
                self.df2.loc[:, 'act'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.df2.loc[:, 'edit_nom'] = self.username

                # print(self.df2.loc[:,['{}'.format(item),'act']])

            if correcto == 0:
                msgBox = QMessageBox()
                msgBox.setIcon(QMessageBox.Information)
                msgBox.setText(error)
                msgBox.setWindowTitle("Conexión")
                msgBox.setStandardButtons(QMessageBox.Ok)
                msgBox.exec()

    ########################################################################
    # Actualización de dataframe a partir de informacion de encuesta (gb)
    ########################################################################

    def onSegmentoEdited(self):
        if self.df2.empty is False:
            text = self.lineEdit_2.text()
            segmento = text
            self.df2.loc[:, 'segmento'] = segmento
        else:
            pass

    def onHogarEdited(self):
        if self.df2.empty is False:
            text = self.lineEdit_3.text()
            hogar = text
            self.df2.loc[:, 'hogar'] = hogar
        else:
            pass

    def ConectarSQL(self):
        self.login = Ui_Login(self)
        self.login.show()

    ########################################################################
    # Funcion para cargar datos
    ########################################################################

    def loadData(self):

        self.tableWidget.setRowCount(0)

        # Carga key desde recuadro
        self.key = self.Buscar_Key.text()

        # Cambia a primera hoja y modifica el tamaño de la columna nombre
        self.tabWidget.setCurrentIndex(0)

        # Define variables a cargar en matriz -> Ojo que numero columna (variables) en matriz esta fijo del comienzo
        self.df2 = self.df.loc[self.df['interview__key'] == self.key]
        self.df2.reset_index(drop=True, inplace=True)
        self.df3 = self.df2.loc[:, [
            'nombre', 'pcoh', 'sexo', 'edad', 'p4a', 'p4b', 'p4b_esp',
            'p4c', 'p4d', 'p5a', 'p5b_c', 'p5b', 'p5c_c', 'p5c',
            'p6a', 'p6a1', 'p6c', 'p6d', 'p6d_esp', 'p6d1']]

        # Actualiza errores y comentarios para encuesta cargada
        self.UpdateErrorsKey()
        self.UpdateComentariosKey()
        self.write_df_to_qtable(self.df3, self.tableWidget, self.df_Errores)

        # Variables para identificacion

        # Segmento
        segmento = self.df2.segmento
        segmento = segmento.values.tolist()[0]
        segmento = str(segmento).replace(".0", "")
        self.lineEdit_2.setText("{}".format(segmento))

        # Coordinador
        coordinador = self.df2.coordinador
        coordinador = coordinador.values.tolist()[0]
        self.lineEdit_4.setText("{}".format(coordinador))

        # Telefono
        telefono = self.df2.telefono1
        telefono = telefono.values.tolist()[0]
        telefono = str('%.0f' % (telefono))
        self.lineEdit_5.setText("{}".format(telefono))

        # Hogar
        hogar = self.df2.hogar
        hogar = hogar.values.tolist()[0]
        hogar = str(hogar).replace(".0", "")
        self.lineEdit_3.setText("{}".format(hogar))

        # Encuestador
        encuestador = self.df2.encuestador
        encuestador = encuestador.values.tolist()[0]
        self.lineEdit_7.setText("{}".format(encuestador))

        # Número de personas
        numpersonas = self.df2.numper
        numpersonas = numpersonas.values.tolist()[0]
        numpersonas = str(numpersonas).replace(".0", "")
        self.lineEdit_8.setText("{}".format(numpersonas))

    def write_df_to_qtable(self, df, table, df_Errores):

        print(df_Errores)
        if len(df_Errores) > 0:
            df_Errores.loc[df_Errores['orden'] == 'hogar', ['orden']] = -8
            df_Errores.loc[df_Errores['orden'] == 'hog', ['orden']] = -8
            tag_rows = df_Errores['orden'].astype(str).str.replace("\.0", "").dropna().astype(int) - 1
            tag_rows = tag_rows.to_list()

        # Reiniciamos los contadores
        self.threshold = df.shape[0] * df.shape[1]
        self.i = 0

        headers = list(df)
        table.setRowCount(df.shape[0])
        table.setColumnCount(df.shape[1])
        table.setHorizontalHeaderLabels(headers)

        self.df_array = df.values
        for row in range(df.shape[0]):
            for col in range(df.shape[1]):

                # Opcion 1
                try:
                    value = str('%.0f' % (self.df_array[row, col]))
                except Exception as exception:
                    value = str(self.df_array[row, col])
                # Opcion 1
                if value != "nan" and value != "none" and value != "":
                    table.setItem(row, col, QTableWidgetItem(value))
                    # table.item(row, col).setBackground(QColor(125,125,125))
                else:
                    item = QTableWidgetItem()
                    item.setText("")
                    # item.setForeground(QColor(255,255,255))
                    # item.setFlags(Qt.ItemIsEnabled)
                    table.setItem(row, col, item)

                if len(df_Errores) > 0 and col == 0:
                    if -9 in tag_rows:
                        # for col in range(df.shape[1]):
                        col = 0
                        # table.item(row, col).setBackground(QColor(255, 0, 0, 127))
                        table.item(row, col).setBackground(QColor(255, 0, 0, 127))

                    if row in tag_rows:
                        # for col in range(df.shape[1]):
                        col = 0
                        table.item(row, col).setBackground(QColor(255, 0, 0, 127))

    ########################################################################
    # Funcion para actulizar errores
    ########################################################################

    def UpdateErrorsKey(self):

        if self.Buscar_Key.text() == self.df2['interview__key'][0]:
            # => DIego 23/03
            if self.EntraActualiza == 1:
                # self.EntraActualiza = 0
                # try:
                # 1) Carga key desde recuadro
                self.key = self.Buscar_Key.text()

                # 2) Encuentra errores en la key

                self.df_Errores = External(self).UpdListError2(self.df2, rev_cod=self.RevCod1, key=self.key)
                
                # print("El codigo en UpdateErrorsKey es" + str(self.RevCod1))
                # self.df2.to_csv("C:\\Users\\usuario\\Downloads\\df2.csv")

                self.df_Errores['orden'] = self.df_Errores['orden'].astype(str).str[:-2]
                self.df_Errores.loc[self.df_Errores['orden'] == 'hog', ['orden']] = 'hogar'
                variables = ['orden', 'error', 'tipo']
                # Variables = ['interview__key', 'orden', 'error','tipo']
                self.df_Errores = self.df_Errores.loc[:, variables]
                # print(df_Errores)
                # => Genera objeto para mostrar datos a mostrar
                self.modelErrores = QStandardItemModel(self)
                # => Itera sobre base para mostrar objeto
                for row in range(len(self.df_Errores)):
                    item = [QStandardItem(str(self.df_Errores.iloc[row, col])) for col in range(len(variables))]
                    for x in item:
                        x.setEditable(False)  # => Para que recuadros no sean editables
                        x.setFont(QFont("Arial", int(10 * factor)))  # => Para ajustar letra
                        x.setSizeHint(QSize(x.sizeHint().width(), 60))
                        # print(item)
                    self.modelErrores.invisibleRootItem().appendRow(
                        item)
                # => Para mostrar nombres de columnas
                self.modelErrores.setHorizontalHeaderLabels(variables)

                # => visible tabla con filtro
                self.listError.setModel(self.modelErrores)

                for x in range(len(variables)):
                    self.listError.setRowHeight(x, 60 * factor)

                # self.listError.setColumnWidth(0,90 * factor)
                self.listError.setColumnWidth(0, 50 * factor)
                self.listError.setColumnWidth(1, 320 * factor)
                self.listError.setColumnWidth(2, 50 * factor)

                self.EntraActualiza = 1

                # self.print_df_to_qtable(self.df2,self.tableWidget, self.df_Errores)
                # except:
                # self.EntraActualiza = 1
        else:
            QMessageBox.warning(self, 'Error de llave de entrevista', 'El interview__key del cuadro de búsqueda no es el mismo interview__key ya cargado', QMessageBox.Ok, QMessageBox.Ok)

    ########################################################################
    # Funcion para actualizar comentarios
    ########################################################################

    def UpdateComentariosKey(self):

        if self.Buscar_Key.text() == self.df2['interview__key'][0]:
            # Nos quedamos con una de las observaciones en un dataframe
            self.df_comentarios = self.df2.drop_duplicates('interview__key', keep="last")

            # Creamos una lista con los comentarios de la validación
            list_comentario = self.df_comentarios['comentarios_validacion']
            list_comentario = list_comentario.values.tolist()[0]
            # print(list_comentario)

            if pd.isnull(list_comentario) is False:
                list_comentario = list_comentario.split("___")
                list_comentario = list(map(lambda x: " ".join(x.split()), list_comentario))
                list_comentario = list(filter(None, list_comentario))
            else:
                list_comentario = ['No hay comentarios de validación']

            comentarios_validacion = []
            for i in range(0, len(list_comentario)):
                comentarios_validacion.append((list_comentario[i], "Validación"))

            # Creamos una lista con los comentarios de SurveySolutions
            list_comentario_2 = self.df_comentarios['comentario_ss']
            list_comentario_2 = list_comentario_2.values.tolist()[0]

            if pd.isnull(list_comentario_2) is False:
                list_comentario_2 = list_comentario_2.split("//")
                list_comentario_2 = list(map(lambda x: " ".join(x.split()), list_comentario_2))
                list_comentario_2 = list(filter(None, list_comentario_2))
            else:
                list_comentario_2 = ['No hay comentarios en SurveySolutions']

            comentarios_ss = []
            for i in range(0, len(list_comentario_2)):
                comentarios_ss.append((list_comentario_2[i], "Survey Solutions"))

            # Creamos lista y dataframe de comentarios:

            comentarios = comentarios_ss + comentarios_validacion
            self.df_comentarios = pd.DataFrame(index=np.arange(0, len(comentarios)), columns=['Comentario', 'Origen'])

            # Agregamos comentarios al dataframe:

            for i in range(0, len(comentarios)):
                self.df_comentarios.iloc[i, 0] = comentarios[i][0]
                self.df_comentarios.iloc[i, 1] = comentarios[i][1]

            # print(self.df_comentarios['Comentario'])

            variables = ['Comentario', 'Origen']

            # => Genera objeto para mostrar datos a mostrar
            self.modelComentarios = QStandardItemModel(self)
            # => Itera sobre base para mostrar objeto
            for row in range(len(self.df_comentarios)):
                item = [QStandardItem(str(self.df_comentarios.iloc[row, col])) for col in range(len(variables))]
                for x in item:
                    x.setEditable(False)  # => Para que recuadros no sean editables
                    x.setFont(QFont("Arial", int(9 * factor)))  # => Para ajustar letra
                self.modelComentarios.invisibleRootItem().appendRow(
                    item)
            # => Para mostrar nombres de columnas
            self.modelComentarios.setHorizontalHeaderLabels(variables)

            # => visible tabla con filtro
            self.viewComentarios.setModel(self.modelComentarios)
            # => Configura tamaño de columnas de tabla con filtro
            for x in range(len(variables)):
                self.view.setColumnWidth(x, 78)
        else:
            pass

    ########################################################################
    # Funcion para agregar comentarios
    ########################################################################

    def AgregarComentario(self):
        self.ventana_agregar_comentario = Ui_Form(self)
        self.ventana_agregar_comentario.show()

    ########################################################################
    # Funcion para actualizar Encuestas
    ########################################################################

    def UpdateErrorsEncuesta(self):

        # 1) Actualiza lista de encuestas

        # => Genera base de datos a mostrar
        d = self.df.drop_duplicates('interview__key', keep="last")  # => Dataframe general Input 1
        d.loc[d.estado == 0, 'estado_s'] = "Sin validar (correcta)"
        d.loc[d.estado == 1, 'estado_s'] = "Sin validar (errores)"
        d.loc[d.estado == 2, 'estado_s'] = "Validada"
        d.loc[d.estado == 3, 'estado_s'] = "Validación forzada"
        variables = ['interview__key', 'segmento', 'coordinador', 'estado_s']  # => Dataframe general Input 2
        lista = d.loc[:, variables]

        # => Genera objeto para mostrar datos a mostrar
        self.model = QStandardItemModel(self)
        # => Itrenra sobre base para mostrar objeto
        for row in range(len(lista)):
            item = [QStandardItem(str(lista.iloc[row, col])) for col in range(len(variables))]
            for x in item:
                x.setEditable(False)  # => Para que recuadros no sean editables
                x.setFont(QFont("Arial", int(9 * factor)))  # => Para ajustar letra
            self.model.invisibleRootItem().appendRow(
                item)
        # => Para mostrar nombres de columnas
        self.model.setHorizontalHeaderLabels(variables)

        # Configuracion general
        # => Define objeto para filtro
        self.proxy = QSortFilterProxyModel(self)
        # => Define lista para el onjeto de filtro
        self.proxy.setSourceModel(self.model)
        # => visible tabla con filtro
        self.view.setModel(self.proxy)
        # => Configura tamaño de columnas de tabla con filtro
        for x in range(len(variables)):
            self.view.setColumnWidth(x, 85 * factor)

        # => Define objeto para filtro
        self.proxy0 = QSortFilterProxyModel(self)
        # => Define lista para el onjeto de filtro
        self.proxy0.setSourceModel(self.proxy)
        # => visible tabla con filtro
        self.view.setModel(self.proxy0)

        # => Al hacer dos click sobre columna interview key, pasarlo a cuadro de texto buscar
        self.view.doubleClicked.connect(self.viewClicked)

        # Agrega nombres de columnas al combobox
        self.comboBox.clear()
        self.comboBox.addItems(["{0}".format(x) for x in variables])
        self.comboBox.setFont(QFont("Arial", int(9 * factor)))

        # Configura que cuadro de texto funcione como texto linkeado al combo box
        self.lineEdit.textChanged.connect(self.on_lineEdit_textChanged)  # => Acción si se escribe texto en el recuadro del filtro
        self.comboBox.currentIndexChanged.connect(self.on_comboBox_currentIndexChanged)  # => Acción si cambia combobox index del filtro

        # configura que se muestre lista al clickear sobre columna
        self.horizontalHeader = self.view.horizontalHeader()
        self.horizontalHeader.sectionClicked.connect(self.on_view_horizontalHeader_sectionClicked)  # =>Muestra listas al hacer click
        self.horizontalHeader.setFont(QFont("Arial", int(9 * factor)))  # => formato para texto que se despliega

    def viewClicked(self, clickedIndex):
        if clickedIndex.column() == 0:
            self.Buscar_Key.setText(clickedIndex.data())

    ########################################################################
    # Acción al cierre
    ########################################################################

    def closeEvent(self, event):
        reply = QMessageBox.question(self, 'Cierre de ventana', '¿Está seguro de que desea cerrar la ventana?', QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if reply == QMessageBox.Yes:
            event.accept()
        else:
            event.ignore()

    ########################################################################
    # Función para descarga de Base Validada (solo admin)
    ########################################################################

    def admin_features(self):
        self.gridLayoutWidget_8 = QWidget(self.tab_comentarios_2)
        self.gridLayoutWidget_8.setGeometry(QRect(10, 250, 321, 181))
        self.gridLayoutWidget_8.setObjectName("gridLayoutWidget_8")
        self.grid_BaseValidada = QGridLayout(self.gridLayoutWidget_8)
        self.grid_BaseValidada.setContentsMargins(0, 0, 0, 0)
        self.grid_BaseValidada.setObjectName("grid_BaseValidada")
        spacerItem2 = QSpacerItem(20, 15, QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.grid_BaseValidada.addItem(spacerItem2, 0, 1, 1, 1)
        spacerItem3 = QSpacerItem(10, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.grid_BaseValidada.addItem(spacerItem3, 2, 0, 1, 1)
        self.line_BaseValidada = QFrame(self.gridLayoutWidget_8)
        self.line_BaseValidada.setFrameShape(QFrame.HLine)
        self.line_BaseValidada.setFrameShadow(QFrame.Sunken)
        self.line_BaseValidada.setObjectName("line_BaseValidada")
        self.grid_BaseValidada.addWidget(self.line_BaseValidada, 2, 1, 1, 1)
        spacerItem4 = QSpacerItem(20, 15, QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.grid_BaseValidada.addItem(spacerItem4, 3, 1, 1, 1)
        spacerItem5 = QSpacerItem(10, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.grid_BaseValidada.addItem(spacerItem5, 2, 2, 1, 1)
        self.groupBox_BaseValidada = QGroupBox(self.gridLayoutWidget_8)
        self.groupBox_BaseValidada.setObjectName("groupBox_BaseValidada")
        self.gridLayoutWidget_9 = QWidget(self.groupBox_BaseValidada)
        self.gridLayoutWidget_9.setGeometry(QRect(60, 30, 211, 71))
        self.gridLayoutWidget_9.setObjectName("gridLayoutWidget_9")
        self.grid_BaseValidada_inner = QGridLayout(self.gridLayoutWidget_9)
        self.grid_BaseValidada_inner.setContentsMargins(0, 0, 0, 0)
        self.grid_BaseValidada_inner.setObjectName("grid_BaseValidada_inner")
        self.BaseValidada_excel = QPushButton(self.gridLayoutWidget_9)
        self.BaseValidada_excel.setObjectName("BaseValidada_excel")
        self.grid_BaseValidada_inner.addWidget(self.BaseValidada_excel, 0, 0, 1, 1)
        self.BaseValidada_stata = QPushButton(self.gridLayoutWidget_9)
        self.BaseValidada_stata.setObjectName("BaseValidada_stata")
        self.grid_BaseValidada_inner.addWidget(self.BaseValidada_stata, 1, 0, 1, 1)
        self.grid_BaseValidada.addWidget(self.groupBox_BaseValidada, 4, 0, 1, 3)
        _translate = QCoreApplication.translate
        self.groupBox_BaseValidada.setTitle(_translate("MainWindow", "Descarga de Base Validada"))
        self.BaseValidada_excel.setText(_translate("MainWindow", "Descargar (Excel/XLSX)"))
        self.BaseValidada_stata.setText(_translate("MainWindow", "Descargar (Stata/DTA)"))

        # Conecta botones a función:
        self.BaseValidada_excel.clicked.connect(self.sql_to_excel)
        self.BaseValidada_stata.clicked.connect(self.sql_to_stata)
        self.BaseValidada_stata.setEnabled(False)

########################################################################
# Clase de cuadro de diálogo para archivos
########################################################################


class App(QWidget):

    def openFileNameDialog(self):
        self.title = 'PyQt5 file dialogs - pythonspot.com'
        self.left = 10
        self.top = 10
        self.width = 640
        self.height = 480

        self.setWindowTitle(self.title)
        self.setGeometry(self.left, self.top, self.width, self.height)
        options = QFileDialog.Options()
        # options |= QFileDialog.DontUseNativeDialog
        fileName, _ = QFileDialog.getOpenFileName(self, "QFileDialog.getOpenFileName()", "", "All Files (*);;Python Files (*.py)", options=options)
        if fileName:
            # print(fileName)
            return fileName
        self.show()

    def saveFileDialog(self):
        self.title = 'Guardar Reporte de Encuestas'
        self.left = 10
        self.top = 10
        self.width = 640
        self.height = 480

        self.setWindowTitle(self.title)
        self.setGeometry(self.left, self.top, self.width, self.height)

        options = QFileDialog.Options()
        # options |= QFileDialog.DontUseNativeDialog
        fileName, _ = QFileDialog.getSaveFileName(self, "QFileDialog.getSaveFileName()", "", "Excel (*.xlsx)", options=options)
        if fileName:
            # print(fileName)
            return fileName
        self.show()

########################################################################
# Clase para guardar cambios realizados sobre IKey
########################################################################


class Ui_Dialog(QDialog):

    def __init__(self, parent=None):
        super(Ui_Dialog, self).__init__(parent)
        self.setupUi(self)

    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.resize(432, 330)
        self.gridLayoutWidget_4 = QWidget(Dialog)
        self.gridLayoutWidget_4.setGeometry(QRect(20, 20, 391, 291))
        self.gridLayoutWidget_4.setObjectName("gridLayoutWidget_4")
        self.gridLayout_4 = QGridLayout(self.gridLayoutWidget_4)
        self.gridLayout_4.setContentsMargins(0, 0, 0, 0)
        self.gridLayout_4.setObjectName("gridLayout_4")
        self.gridLayout = QGridLayout()
        self.gridLayout.setObjectName("gridLayout")
        self.groupBox_2 = QGroupBox(self.gridLayoutWidget_4)
        self.groupBox_2.setEnabled(True)
        self.groupBox_2.setObjectName("groupBox_2")
        self.gridLayoutWidget_3 = QWidget(self.groupBox_2)
        self.gridLayoutWidget_3.setGeometry(QRect(20, 30, 351, 71))
        self.gridLayoutWidget_3.setObjectName("gridLayoutWidget_3")
        self.gridLayout_3 = QGridLayout(self.gridLayoutWidget_3)
        self.gridLayout_3.setContentsMargins(0, 0, 0, 0)
        self.gridLayout_3.setObjectName("gridLayout_3")
        self.comentario_vf = QTextEdit(self.gridLayoutWidget_3)
        self.comentario_vf.setObjectName("comentario_vf")
        self.comentario_vf.setEnabled(False)
        self.gridLayout_3.addWidget(self.comentario_vf, 0, 0, 1, 1)
        self.gridLayout.addWidget(self.groupBox_2, 1, 0, 1, 1)
        self.groupBox = QGroupBox(self.gridLayoutWidget_4)
        self.groupBox.setObjectName("groupBox")
        self.gridLayoutWidget_2 = QWidget(self.groupBox)
        self.gridLayoutWidget_2.setGeometry(QRect(10, 30, 321, 76))
        self.gridLayoutWidget_2.setObjectName("gridLayoutWidget_2")
        self.gridLayout_2 = QGridLayout(self.gridLayoutWidget_2)
        self.gridLayout_2.setContentsMargins(0, 0, 0, 0)
        self.gridLayout_2.setObjectName("gridLayout_2")
        self.radioButton_2 = QRadioButton(self.gridLayoutWidget_2)
        self.radioButton_2.setObjectName("radioButton_2")
        self.gridLayout_2.addWidget(self.radioButton_2, 1, 0, 1, 1)
        self.radioButton = QRadioButton(self.gridLayoutWidget_2)
        self.radioButton.setObjectName("radioButton")
        self.gridLayout_2.addWidget(self.radioButton, 0, 0, 1, 1)
        self.radioButton_3 = QRadioButton(self.gridLayoutWidget_2)
        self.radioButton_3.setObjectName("radioButton_3")
        self.gridLayout_2.addWidget(self.radioButton_3, 2, 0, 1, 1)
        self.gridLayout.addWidget(self.groupBox, 0, 0, 1, 1)
        self.gridLayout_4.addLayout(self.gridLayout, 0, 0, 1, 1)
        self.buttonBox = QDialogButtonBox(self.gridLayoutWidget_4)
        self.buttonBox.setOrientation(Qt.Horizontal)
        self.buttonBox.setStandardButtons(QDialogButtonBox.Cancel | QDialogButtonBox.Ok)
        self.buttonBox.setObjectName("buttonBox")
        self.gridLayout_4.addWidget(self.buttonBox, 1, 0, 1, 1)

        self.retranslateUi(Dialog)
        self.buttonBox.accepted.connect(self.guarda)
        self.buttonBox.rejected.connect(self.cancela)
        self.buttonBox.setEnabled(False)
        QMetaObject.connectSlotsByName(Dialog)

        self.radioButton.toggled.connect(lambda: self.btnstate(self.radioButton))
        self.radioButton_2.toggled.connect(lambda: self.btnstate(self.radioButton_2))
        self.radioButton_3.toggled.connect(lambda: self.btnstate(self.radioButton_3))

        # Variable para cambiar estado de encuesta:
        self.estado_guardar = None

        # Variable que comprueba si tiene errores:
        self.n_errores = self.parent().n_errores
        if self.n_errores > 0:
            self.radioButton_2.setEnabled(False)

    def guarda(self):
        try:
            if self.comentario_vf.toPlainText() != "":
                comentario = self.comentario_vf.toPlainText()
                if list(self.parent().df2.comentarios_validacion)[0] == "No hay comentarios de validación":
                    self.parent().df2.loc[:, 'comentarios_validacion'] = comentario
                    # print(self.parent().df2['comentarios_validacion'])
                else:
                    self.parent().df2.loc[:, 'comentarios_validacion'] = self.parent().df2['comentarios_validacion'].astype(str) + "___" + comentario
                    self.parent().UpdateComentariosKey()
                    # print(self.parent().df2['comentarios_validacion'])

            self.parent().UpdateComentariosKey()
            self.parent().df2.loc[:, 'estado'] = self.estado_guardar
            self.parent().df2.loc[:, 'act'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.parent().df2.loc[:, 'edit_nom'] = self.parent().username
            engine = FunVal.SqlAlchemyEngine(text_user, pass_sql, base_sql)
            # print(self.parent().df2)
            self.parent().df2.to_sql(f'{table_sql}', con=engine, if_exists='append', chunksize=10, index=False)
            # print("Actualizado correctamente en SQL")
            interview__key = self.parent().df2.interview__key[0]
            indexNames = self.parent().df[self.parent().df['interview__key'] == interview__key].index
            self.parent().df.drop(indexNames, inplace=True)
            # Chequea si aun quedan:
            self.parent().df = self.parent().df.append(self.parent().df2)
            # print("Actualizado correctamente en base local")
            QMessageBox.information(self, 'Guardado exitosamente', 'La información fue guardada correctamente', QMessageBox.Ok, QMessageBox.Ok)
            self.close()
        except Exception as exception:
            print(exception)
            QMessageBox.warning(self, 'No se ha podido guardar', 'La información no ha podido ser guardada correctamente. Recuerde tener una conexión activa de internet al momento de guardar.', QMessageBox.Ok, QMessageBox.Ok)
            self.close()

    def cancela(self):
        self.close()

    def btnstate(self, b):
        if b.text() == "No validar":
            if b.isChecked() is True:
                self.buttonBox.setEnabled(True)
                # print(b.text()+" is selected")
                # print(self.parent().df2['estado'])
                self.estado_guardar = self.parent().df2['estado'].astype(str)[0]
                if "." in self.estado_guardar:
                    self.estado_guardar = self.estado_guardar[:-2]
                if self.n_errores > 0:
                    self.estado_guardar = 1
                    QMessageBox.warning(self, 'Estado de encuesta', 'Debido a que su encuesta tiene errores, el estado de su encuesta pasará a "Sin validar (errores)" si utiliza esta opción de validación', QMessageBox.Ok, QMessageBox.Ok)
                # print(self.estado_guardar)
                self.comentario_vf.setText("")
                self.comentario_vf.setEnabled(False)
            else:
                print(b.text() + " is deselected")

        if b.text() == "Validar":
            if b.isChecked() is True:
                self.buttonBox.setEnabled(True)
                # print(b.text()+" is selected")
                self.estado_guardar = 2
                self.comentario_vf.setText("")
                self.comentario_vf.setEnabled(False)
            else:
                print(b.text() + " is deselected")
        if b.text() == "Validar (validación forzada)":
            if b.isChecked() is True:
                self.buttonBox.setEnabled(True)
                # print(b.text()+" is selected")
                self.estado_guardar = 3
                self.comentario_vf.setEnabled(True)
            else:
                print(b.text() + " is deselected")

    def retranslateUi(self, Dialog):
        _translate = QCoreApplication.translate
        Dialog.setWindowTitle(_translate("Dialog", "Guardar cambios"))
        self.groupBox_2.setTitle(_translate("Dialog", "Comentario (validación forzada)"))
        self.groupBox.setTitle(_translate("Dialog", "Opciones de validación"))
        self.radioButton_2.setText(_translate("Dialog", "Validar"))
        self.radioButton.setText(_translate("Dialog", "No validar"))
        self.radioButton_3.setText(_translate("Dialog", "Validar (validación forzada)"))

########################################################################
# Clase para agregar comentario sobre encuesta
########################################################################


class Ui_Form(QDialog):
    def __init__(self, parent=None):
        super(Ui_Form, self).__init__(parent)
        self.setupUi(self)

    def setupUi(self, Form):
        Form.setObjectName("Form")
        Form.resize(400, 300)
        self.gridLayoutWidget = QWidget(Form)
        self.gridLayoutWidget.setGeometry(QRect(0, 0, 401, 301))
        self.gridLayoutWidget.setObjectName("gridLayoutWidget")
        self.gridLayout = QGridLayout(self.gridLayoutWidget)
        self.gridLayout.setContentsMargins(0, 0, 0, 0)
        self.gridLayout.setObjectName("gridLayout")
        self.textEdit = QTextEdit(self.gridLayoutWidget)
        self.textEdit.setObjectName("textEdit")
        self.gridLayout.addWidget(self.textEdit, 0, 0, 1, 1)
        self.pushButton = QPushButton(self.gridLayoutWidget)
        self.pushButton.setObjectName("pushButton")
        self.pushButton.setEnabled(False)
        self.gridLayout.addWidget(self.pushButton, 1, 0, 1, 1)
        self.pushButton.clicked.connect(self.comentario)
        self.textEdit.textChanged.connect(self.textocambiado)
        self.retranslateUi(Form)
        QMetaObject.connectSlotsByName(Form)

    def textocambiado(self):
        self.pushButton.setEnabled(True)

    def comentario(self):
        if self.textEdit.toPlainText() != "":
            comentario = self.textEdit.toPlainText()
            # print(pd.isnull(list(self.parent().df2.comentarios_validacion)[0]))
            if list(self.parent().df2.comentarios_validacion)[0] == "No hay comentarios de validación":
                self.parent().df2.loc[:, 'comentarios_validacion'] = comentario
                # print(self.parent().df2['comentarios_validacion'])
            else:
                self.parent().df2.loc[:, 'comentarios_validacion'] = self.parent().df2['comentarios_validacion'].astype(str) + "___" + comentario
                self.parent().UpdateComentariosKey()
                # print(self.parent().df2['comentarios_validacion'])

            # print(self.parent().df2['comentarios_validacion'])
            self.parent().UpdateComentariosKey()
            self.close()
        else:
            QMessageBox.warning(self, 'Comentario vacío', 'Está intentando agregar un comentario vacío, si desea agregar un comentario, por favor escriba en el recuadro superior', QMessageBox.Ok, QMessageBox.Ok)
            self.pushButton.setEnabled(True)

    def retranslateUi(self, Form):
        _translate = QCoreApplication.translate
        Form.setWindowTitle(_translate("Form", "Agregar comentario a encuesta"))
        self.pushButton.setText(_translate("Form", "Agregar"))

########################################################################
# Clase para agregar segundo hogar a vivienda
#######################################################################


class Ui_SegHog(QDialog):

    def __init__(self, parent=None):
        super(Ui_SegHog, self).__init__(parent)
        self.setupUi(self)

    def setupUi(self, SegHog):
        SegHog.setObjectName("SegHog")
        SegHog.resize(411, 281)
        SegHog.setSizeGripEnabled(False)
        SegHog.setModal(False)
        self.groupBox = QGroupBox(SegHog)
        self.groupBox.setGeometry(QRect(20, 20, 371, 201))
        self.groupBox.setObjectName("groupBox")
        self.gridLayoutWidget = QWidget(self.groupBox)
        self.gridLayoutWidget.setGeometry(QRect(10, 20, 351, 171))
        self.gridLayoutWidget.setObjectName("gridLayoutWidget")
        self.gridLayout = QGridLayout(self.gridLayoutWidget)
        self.gridLayout.setContentsMargins(0, 0, 0, 0)
        self.gridLayout.setObjectName("gridLayout")
        self.lineEdit = QLineEdit(self.gridLayoutWidget)
        self.lineEdit.setMaximumSize(QSize(80, 16777215))
        self.lineEdit.setEchoMode(QLineEdit.Normal)
        self.lineEdit.setCursorPosition(0)
        self.lineEdit.setAlignment(Qt.AlignCenter)
        self.lineEdit.setPlaceholderText("")
        self.lineEdit.setObjectName("lineEdit")
        self.gridLayout.addWidget(self.lineEdit, 1, 1, 1, 1)
        self.checkBox = QCheckBox(self.gridLayoutWidget)
        font = QFont()
        font.setPointSize(8)
        self.checkBox.setFont(font)
        self.checkBox.setObjectName("checkBox")
        self.gridLayout.addWidget(self.checkBox, 2, 1, 1, 1)
        self.lineEdit_2 = QLineEdit(self.gridLayoutWidget)
        self.lineEdit_2.setMaximumSize(QSize(80, 16777215))
        self.lineEdit_2.setAlignment(Qt.AlignCenter)
        self.lineEdit_2.setObjectName("lineEdit_2")
        self.gridLayout.addWidget(self.lineEdit_2, 4, 1, 1, 1)
        self.label_2 = QLabel(self.gridLayoutWidget)
        self.label_2.setObjectName("label_2")
        self.gridLayout.addWidget(self.label_2, 3, 1, 1, 1)
        self.label = QLabel(self.gridLayoutWidget)
        self.label.setObjectName("label")
        self.gridLayout.addWidget(self.label, 0, 1, 1, 1)
        self.checkBox_2 = QCheckBox(self.gridLayoutWidget)
        font = QFont()
        font.setPointSize(8)
        self.checkBox_2.setFont(font)
        self.checkBox_2.setObjectName("checkBox_2")
        self.gridLayout.addWidget(self.checkBox_2, 5, 1, 1, 1)
        self.horizontalLayoutWidget = QWidget(SegHog)
        self.horizontalLayoutWidget.setGeometry(QRect(20, 230, 371, 31))
        self.horizontalLayoutWidget.setObjectName("horizontalLayoutWidget")
        self.horizontalLayout = QHBoxLayout(self.horizontalLayoutWidget)
        self.horizontalLayout.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout.setObjectName("horizontalLayout")
        spacerItem = QSpacerItem(80, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem)
        self.pushButton = QPushButton(self.horizontalLayoutWidget)
        self.pushButton.setObjectName("pushButton")
        self.horizontalLayout.addWidget(self.pushButton)
        spacerItem1 = QSpacerItem(80, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem1)

        self.checkBox.stateChanged.connect(self.checkbox)
        self.checkBox_2.stateChanged.connect(self.checkbox2)
        self.pushButton.clicked.connect(self.boton_rellenar)
        self.retranslateUi(SegHog)
        QMetaObject.connectSlotsByName(SegHog)

    def checkbox(self, state):
        try:
            if state == Qt.Checked:
                self.key_segundo_hogar = str(self.parent().Buscar_Key.text())
                self.lineEdit.setEnabled(False)
                self.key_segundo_hogar.replace("-", "")
                self.lineEdit.setText(self.key_segundo_hogar)
                self.lineEdit_2.setText("")
                self.checkBox_2.setChecked(False)
            else:
                self.lineEdit.setEnabled(True)
                self.lineEdit.setText("")
        except Exception as exception:
            print(exception)
            QMessageBox.warning(self, 'No se ha podido cargar Interview Key', 'No se ha podido utilizar el Interview Key cargado. Por favor, ingrese manualmente.', QMessageBox.Ok, QMessageBox.Ok)
            self.close()

    def checkbox2(self, state):
        try:
            if state == Qt.Checked:
                self.key_hogar_original = str(self.parent().Buscar_Key.text())
                self.lineEdit_2.setEnabled(False)
                self.key_hogar_original.replace("-", "")
                self.lineEdit_2.setText(self.key_hogar_original)
                self.lineEdit.setText("")
                self.checkBox.setChecked(False)
            else:
                self.lineEdit_2.setEnabled(True)
                self.lineEdit_2.setText("")
        except Exception as exception:
            print(exception)
            QMessageBox.warning(self, 'No se ha podido cargar Interview Key', 'No se ha podido utilizar el Interview Key cargado. Por favor, ingrese manualmente.', QMessageBox.Ok, QMessageBox.Ok)
            self.close()

    def boton_rellenar(self):

        interview_key_hogarprincipal = self.lineEdit_2.text()
        interview_key_segundohogar = self.lineEdit.text()

        var = ["cuarto", "estrato", "comuna", "segmento", "hogar", "encuesta", "gse", "comunat", "direccion", "coordinador", "obs", "nombre_enc", "jefe_hogar"]
        identificacion = self.parent().df_or.loc[self.parent().df_or['interview__key'] == interview_key_hogarprincipal, var].values.tolist()[0]

        for i in range(0, len(var)):
            if var[i] == 'hogar':
                identificacion[i] = str(identificacion[i]).replace(".0", "")

                # Obtenemos el segmento completo, y los dígitos anteriores al identificador de hogar (último dígito):
                segmento = identificacion[3]
                hogar = int(str(identificacion[4]).replace(".0", "")[:-1])

                # Creamos el id del hogar como segmento + 0 + hogar
                if hogar < 10:
                    id_seg_hog = segmento + '0' + str(hogar)
                else:
                    id_seg_hog = segmento + str(hogar)

                # Creamos una variable local con una lista de todas las encuestas:
                list_encuestas = self.parent().df_or.encuesta.values.tolist()

                # Creamos una lista con todos los valores que coinciden con el valor de id_seg_hog creado anteriormente:
                valores_encuestas = []
                for j in range(0, len(list_encuestas)):
                    tupla_encuestas = ((str(list_encuestas[j]).replace(".0", ""), str(list_encuestas[j]).replace(".0", "")[:-1]))
                    if id_seg_hog == tupla_encuestas[1]:
                        if tupla_encuestas[0] not in valores_encuestas:
                            valores_encuestas.append(tupla_encuestas[0])

                # Vemos cuál es el máximo último dígito para las encuestas:
                max_hogar_encuesta = max(list(map(lambda x: int(x[-1]), valores_encuestas)))
                hogar = identificacion[i][:len(identificacion[i]) - 1] + str(max_hogar_encuesta + 1)
                self.parent().df.loc[self.parent().df['interview__key'] == interview_key_segundohogar, '{}'.format(var[i])] = hogar
            elif var[i] == 'encuesta':
                # print("La encuesta es: " + id_seg_hog + str(max_hogar_encuesta+1))
                encuesta = int(id_seg_hog + str(max_hogar_encuesta + 1))
                self.parent().df.loc[self.parent().df['interview__key'] == interview_key_segundohogar, '{}'.format(var[i])] = encuesta
            elif var[i] == 'nombre_enc':
                nombre_enc = self.parent().df.loc[(self.parent().df['interview__key'] == interview_key_segundohogar) & (np.isfinite(self.parent().df['q_resp'])), 'nombre'].values.tolist()[0]
                self.parent().df.loc[self.parent().df['interview__key'] == interview_key_segundohogar, '{}'.format(var[i])] = nombre_enc
            elif var[i] == 'jefe_hogar':
                jefe_hogar = self.parent().df.loc[(self.parent().df['interview__key'] == interview_key_segundohogar) & (self.parent().df['pcoh'] == 1), 'nombre'].values.tolist()[0]
                self.parent().df.loc[self.parent().df['interview__key'] == interview_key_segundohogar, '{}'.format(var[i])] = jefe_hogar
            elif var[i] == 'cuarto':
                cuarto = int(str(identificacion[i]).replace(".0", ""))
                self.parent().df['cuarto'] = self.parent().df['cuarto'].astype('int64')
                self.parent().df.loc[self.parent().df['interview__key'] == interview_key_segundohogar, '{}'.format(var[i])] = cuarto
            else:
                self.parent().df.loc[self.parent().df['interview__key'] == interview_key_segundohogar, '{}'.format(var[i])] = '{}'.format(identificacion[i])
            # print(var[i] + " es")
            # print(self.parent().df_or.loc[self.parent().df_or['interview__key'] == interview_key_hogarprincipal,'{}'.format(var[i])])
            # print(self.parent().df.loc[self.parent().df['interview__key'] == interview_key_segundohogar,'{}'.format(var[i])])

        # Para el IK de Segundo Hogar, primero lo buscamos en la base de observaciones con tipo de muestra == 2
        self.df_ik_seghog = self.parent().df.loc[self.parent().df['interview__key'] == interview_key_segundohogar, :]
        # Actualizamos la fecha y el usuario:
        self.df_ik_seghog.loc[:, 'act'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.df_ik_seghog.loc[:, 'edit_nom'] = self.parent().username

        # print(self.df_ik_seghog[['interview__key','act']])
        # Hacemos los cambios en el SQL:

        # Agregar cuadro de dialogo para confirmar si desea guardar cambios

        engine = FunVal.SqlAlchemyEngine(text_user, pass_sql, base_sql)
        self.df_ik_seghog.to_sql(f'{table_sql}', con=engine, if_exists='append', chunksize=10, index=False)
        print("Actualizado correctamente en SQL")
        QMessageBox.information(self, 'Guardado exitosamente', 'La información fue guardada correctamente', QMessageBox.Ok, QMessageBox.Ok)

    def retranslateUi(self, SegHog):
        _translate = QCoreApplication.translate
        SegHog.setWindowTitle(_translate("SegHog", "Información de Segundo Hogar"))
        self.groupBox.setTitle(_translate("SegHog", "Rellenar Información de Identificación de Segundo Hogar"))
        self.lineEdit.setInputMask(_translate("SegHog", "99-99-99-99"))
        self.checkBox.setText(_translate("SegHog", "Utilizar Interview Key de encuesta cargada actualmente"))
        self.lineEdit_2.setInputMask(_translate("SegHog", "99-99-99-99"))
        self.label_2.setText(_translate("SegHog", "Interview Key de Hogar Principal:"))
        self.label.setText(_translate("SegHog", "Interview Key de Segundo Hogar:"))
        self.pushButton.setText(_translate("SegHog", "Rellenar"))
        self.checkBox_2.setText(_translate("ControlOriginal", "Utilizar Interview Key de encuesta cargada actualmente"))

########################################################################
# Clase para reemplazar encuesta original por encuesta de control
#######################################################################


class Ui_ControlOriginal(QDialog):

    def __init__(self, parent=None):
        super(Ui_ControlOriginal, self).__init__(parent)
        self.setupUi(self)

    def setupUi(self, ControlOriginal):
        ControlOriginal.setObjectName("ControlOriginal")
        ControlOriginal.resize(411, 281)
        ControlOriginal.setSizeGripEnabled(False)
        ControlOriginal.setModal(False)
        self.groupBox = QGroupBox(ControlOriginal)
        self.groupBox.setGeometry(QRect(20, 20, 371, 201))
        self.groupBox.setObjectName("groupBox")
        self.gridLayoutWidget = QWidget(self.groupBox)
        self.gridLayoutWidget.setGeometry(QRect(10, 20, 351, 171))
        self.gridLayoutWidget.setObjectName("gridLayoutWidget")
        self.gridLayout = QGridLayout(self.gridLayoutWidget)
        self.gridLayout.setContentsMargins(0, 0, 0, 0)
        self.gridLayout.setObjectName("gridLayout")
        self.lineEdit = QLineEdit(self.gridLayoutWidget)
        self.lineEdit.setMaximumSize(QSize(80, 16777215))
        self.lineEdit.setEchoMode(QLineEdit.Normal)
        self.lineEdit.setCursorPosition(0)
        self.lineEdit.setAlignment(Qt.AlignCenter)
        self.lineEdit.setPlaceholderText("")
        self.lineEdit.setObjectName("lineEdit")
        self.gridLayout.addWidget(self.lineEdit, 1, 1, 1, 1)
        self.checkBox = QCheckBox(self.gridLayoutWidget)
        font = QFont()
        font.setPointSize(8)
        self.checkBox.setFont(font)
        self.checkBox.setObjectName("checkBox")
        self.gridLayout.addWidget(self.checkBox, 2, 1, 1, 1)
        self.lineEdit_2 = QLineEdit(self.gridLayoutWidget)
        self.lineEdit_2.setMaximumSize(QSize(80, 16777215))
        self.lineEdit_2.setAlignment(Qt.AlignCenter)
        self.lineEdit_2.setObjectName("lineEdit_2")
        self.gridLayout.addWidget(self.lineEdit_2, 4, 1, 1, 1)
        self.label_2 = QLabel(self.gridLayoutWidget)
        self.label_2.setObjectName("label_2")
        self.gridLayout.addWidget(self.label_2, 3, 1, 1, 1)
        self.label = QLabel(self.gridLayoutWidget)
        self.label.setObjectName("label")
        self.gridLayout.addWidget(self.label, 0, 1, 1, 1)
        self.checkBox_2 = QCheckBox(self.gridLayoutWidget)
        font = QFont()
        font.setPointSize(8)
        self.checkBox_2.setFont(font)
        self.checkBox_2.setObjectName("checkBox_2")
        self.gridLayout.addWidget(self.checkBox_2, 5, 1, 1, 1)
        self.horizontalLayoutWidget = QWidget(ControlOriginal)
        self.horizontalLayoutWidget.setGeometry(QRect(20, 230, 371, 31))
        self.horizontalLayoutWidget.setObjectName("horizontalLayoutWidget")
        self.horizontalLayout = QHBoxLayout(self.horizontalLayoutWidget)
        self.horizontalLayout.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout.setObjectName("horizontalLayout")
        spacerItem = QSpacerItem(80, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem)
        self.pushButton = QPushButton(self.horizontalLayoutWidget)
        self.pushButton.setObjectName("pushButton")
        self.horizontalLayout.addWidget(self.pushButton)
        spacerItem1 = QSpacerItem(80, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem1)

        self.checkBox.stateChanged.connect(self.checkbox)
        self.checkBox_2.stateChanged.connect(self.checkbox2)
        self.pushButton.clicked.connect(self.guarda)
        self.retranslateUi(ControlOriginal)
        QMetaObject.connectSlotsByName(ControlOriginal)

    def retranslateUi(self, ControlOriginal):
        _translate = QCoreApplication.translate
        ControlOriginal.setWindowTitle(_translate("ControlOriginal", "Reemplazar Encuesta de Control por Encuesta Original"))
        self.groupBox.setTitle(_translate("ControlOriginal", "Reemplazar Encuesta de Control por Encuesta Original"))
        self.lineEdit.setInputMask(_translate("ControlOriginal", "99-99-99-99"))
        self.checkBox.setText(_translate("ControlOriginal", "Utilizar Interview Key de encuesta cargada actualmente"))
        self.lineEdit_2.setInputMask(_translate("ControlOriginal", "99-99-99-99"))
        self.label_2.setText(_translate("ControlOriginal", "Interview Key de Original:"))
        self.label.setText(_translate("ControlOriginal", "Interview Key de Control:"))
        self.checkBox_2.setText(_translate("ControlOriginal", "Utilizar Interview Key de encuesta cargada actualmente"))
        self.pushButton.setText(_translate("ControlOriginal", "Reemplazar"))

    def checkbox(self, state):
        try:
            if state == Qt.Checked:
                self.key_control = str(self.parent().Buscar_Key.text())
                self.lineEdit.setEnabled(False)
                self.key_control.replace("-", "")
                self.lineEdit.setText(self.key_control)
                self.lineEdit_2.setText("")
                self.checkBox_2.setChecked(False)
            else:
                self.lineEdit.setEnabled(True)
                self.lineEdit.setText("")
        except Exception as exception:
            print(exception)
            QMessageBox.warning(self, 'No se ha podido cargar Interview Key', 'No se ha podido utilizar el Interview Key cargado. Por favor, ingrese manualmente.', QMessageBox.Ok, QMessageBox.Ok)
            self.close()

    def checkbox2(self, state):
        try:
            if state == Qt.Checked:
                self.key_original = str(self.parent().Buscar_Key.text())
                self.lineEdit_2.setEnabled(False)
                self.key_original.replace("-", "")
                self.lineEdit_2.setText(self.key_original)
                self.lineEdit.setText("")
                self.checkBox.setChecked(False)
            else:
                self.lineEdit_2.setEnabled(True)
                self.lineEdit_2.setText("")
        except Exception as exception:
            print(exception)
            QMessageBox.warning(self, 'No se ha podido cargar Interview Key', 'No se ha podido utilizar el Interview Key cargado. Por favor, ingrese manualmente.', QMessageBox.Ok, QMessageBox.Ok)
            self.close()

    def guarda(self):
        try:
            # Recuperamos IK desde cuadros:
            interview_key_control = self.lineEdit.text()
            interview_key_original = self.lineEdit_2.text()

            # print(interview_key_control)
            # print(interview_key_original)

            # Para el IK Original, primero lo buscamos en la base de observaciones con tipo de muestra == 1
            self.df_ik_original = self.parent().df_or.loc[self.parent().df_or['interview__key'] == interview_key_original, :]
            # Actualizamos la fecha:
            self.df_ik_original['tipo_muestra'] = -9
            self.df_ik_original['tipo_muestra'] = self.df_ik_original['tipo_muestra'].astype(str).str.replace("\.0", "").astype(int)
            # print(self.df_ik_original[['interview__key','tipo_muestra']])
            self.df_ik_original.loc[:, 'act'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.df_ik_original.loc[:, 'edit_nom'] = self.parent().username
            # print(self.df_ik_original[['interview__key','act']])
            # Hacemos los cambios en el SQL:
            engine = FunVal.SqlAlchemyEngine(text_user, pass_sql, base_sql)
            self.df_ik_original.to_sql(f'{table_sql}', con=engine, if_exists='append', chunksize=10, index=False)
            # print("Actualizado correctamente en SQL")

            # Lista de interview__key en base principal
            lista_ik = self.parent().df.interview__key.values.tolist()

            if interview_key_original in lista_ik:
                indexNames = self.parent().df[self.parent().df['interview__key'] == interview_key_original].index
                self.parent().df.drop(indexNames, inplace=True)
                self.parent().df = self.parent().df.append(self.df_ik_original)
                self.parent().df['tipo_muestra'] = self.parent().df['tipo_muestra'].astype(str).str.replace("\.0", "").astype(int)

            # Para el IK de Control, lo buscamos en la base cargada:
            self.df_ik_control = self.parent().df.loc[self.parent().df['interview__key'] == interview_key_control, :]
            self.df_ik_control['tipo_muestra'] = 1
            self.df_ik_control['tipo_muestra'] = self.df_ik_control['tipo_muestra'].astype(str).str.replace("\.0", "").astype(int)
            # print(self.df_ik_control[['interview__key','tipo_muestra']])
            self.df_ik_control.loc[:, 'act'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.df_ik_control.loc[:, 'edit_nom'] = self.parent().username
            # print(self.df_ik_control[['interview__key','act']])
            # Hacemos los cambios en el SQL:
            engine = FunVal.SqlAlchemyEngine(text_user, pass_sql, base_sql)
            self.df_ik_control.to_sql(f'{table_sql}', con=engine, if_exists='append', chunksize=10, index=False)
            # print("Actualizado correctamente en SQL")

            if interview_key_control in lista_ik:
                indexNames = self.parent().df[self.parent().df['interview__key'] == interview_key_control].index
                self.parent().df.drop(indexNames, inplace=True)
                self.parent().df = self.parent().df.append(self.df_ik_control)
                self.parent().df['tipo_muestra'] = self.parent().df['tipo_muestra'].astype(str).str.replace("\.0", "").astype(int)

            # Extra
            # print("Nueva base de control")
            # print(self.parent().df.loc[self.parent().df['interview__key'] == interview_key_control, ['interview__key','tipo_muestra','act']])
            # print("Nueva base original")
            # print(self.parent().df.loc[self.parent().df['interview__key'] == interview_key_original, ['interview__key','tipo_muestra','act']])

            QMessageBox.information(self, 'Guardado exitosamente', 'La información fue guardada correctamente', QMessageBox.Ok, QMessageBox.Ok)
            # self.close()
        except Exception as exception:
            print(exception)
            QMessageBox.warning(self, 'No se ha podido guardar', 'La información no ha podido ser guardada correctamente. Recuerde tener una conexión activa de internet al momento de guardar.', QMessageBox.Ok, QMessageBox.Ok)
            self.close()

########################################################################
# Clase para login
#######################################################################


class Ui_Login(QDialog):

    def __init__(self, parent=None):
        super(Ui_Login, self).__init__(parent)
        self.setupUi(self)
        
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.resize(391, 421)
        Dialog.move(800*factor,100*factor)
        sizePolicy = QSizePolicy(QSizePolicy.Preferred, QSizePolicy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(Dialog.sizePolicy().hasHeightForWidth())
        Dialog.setSizePolicy(sizePolicy)
        Dialog.setMouseTracking(False)
        Dialog.setTabletTracking(False)
        Dialog.setAcceptDrops(False)
        Dialog.setStyleSheet(
            "background-color: rgb(255, 255, 255);\n"
            "selection-color: rgb(255, 85, 255);\n"
            "selection-background-color: rgb(255, 0, 0);\n"
            "border-color: rgb(85, 255, 0);\n"
            "alternate-background-color: rgb(85, 255, 255);\n"
            "border-left-color: rgb(0, 85, 255);"
            )
        Dialog.setSizeGripEnabled(False)
        Dialog.setModal(False)
        self.gridLayoutWidget_2 = QWidget(Dialog)
        self.gridLayoutWidget_2.setGeometry(QRect(10, 10, 371, 391))
        self.gridLayoutWidget_2.setObjectName("gridLayoutWidget_2")
        self.gridLayout_6 = QGridLayout(self.gridLayoutWidget_2)
        self.gridLayout_6.setContentsMargins(0, 0, 0, 0)
        self.gridLayout_6.setObjectName("gridLayout_6")
        self.verticalLayout = QVBoxLayout()
        self.verticalLayout.setObjectName("verticalLayout")
        spacerItem = QSpacerItem(20, 10, QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.verticalLayout.addItem(spacerItem)
        self.label_3 = QLabel(self.gridLayoutWidget_2)
        sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.label_3.sizePolicy().hasHeightForWidth())
        self.label_3.setSizePolicy(sizePolicy)
        self.label_3.setLayoutDirection(Qt.LeftToRight)
        self.label_3.setFrameShape(QFrame.NoFrame)
        self.label_3.setFrameShadow(QFrame.Plain)
        self.label_3.setTextFormat(Qt.RichText)
        self.label_3.setScaledContents(False)
        self.label_3.setAlignment(Qt.AlignCenter)
        self.label_3.setObjectName("label_3")
        self.verticalLayout.addWidget(self.label_3)
        self.gridLayout_7 = QGridLayout()
        self.gridLayout_7.setObjectName("gridLayout_7")
        self.line_2 = QFrame(self.gridLayoutWidget_2)
        self.line_2.setFrameShape(QFrame.HLine)
        self.line_2.setFrameShadow(QFrame.Sunken)
        self.line_2.setObjectName("line_2")
        self.gridLayout_7.addWidget(self.line_2, 1, 1, 1, 1)
        spacerItem1 = QSpacerItem(20, 10, QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.gridLayout_7.addItem(spacerItem1, 3, 1, 1, 1)
        spacerItem2 = QSpacerItem(50, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_7.addItem(spacerItem2, 1, 0, 1, 1)
        spacerItem3 = QSpacerItem(20, 25, QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.gridLayout_7.addItem(spacerItem3, 0, 1, 1, 1)
        spacerItem4 = QSpacerItem(50, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_7.addItem(spacerItem4, 4, 2, 1, 1)
        self.label_4 = QLabel(self.gridLayoutWidget_2)
        sizePolicy = QSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.label_4.sizePolicy().hasHeightForWidth())
        self.label_4.setSizePolicy(sizePolicy)
        self.label_4.setObjectName("label_4")
        self.gridLayout_7.addWidget(self.label_4, 2, 1, 1, 1)
        spacerItem5 = QSpacerItem(50, 5, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_7.addItem(spacerItem5, 1, 2, 1, 1)
        spacerItem6 = QSpacerItem(50, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_7.addItem(spacerItem6, 4, 0, 1, 1)
        self.gridLayout_8 = QGridLayout()
        self.gridLayout_8.setObjectName("gridLayout_8")
        self.formLayout_3 = QFormLayout()
        self.formLayout_3.setObjectName("formLayout_3")
        self.usuarioLabel_2 = QLabel(self.gridLayoutWidget_2)
        self.usuarioLabel_2.setObjectName("usuarioLabel_2")
        self.formLayout_3.setWidget(0, QFormLayout.LabelRole, self.usuarioLabel_2)
        self.usuarioLineEdit_2 = QLineEdit(self.gridLayoutWidget_2)
        self.usuarioLineEdit_2.setObjectName("usuarioLineEdit_2")
        self.formLayout_3.setWidget(0, QFormLayout.FieldRole, self.usuarioLineEdit_2)
        self.contraseALabel_2 = QLabel(self.gridLayoutWidget_2)
        sizePolicy = QSizePolicy(QSizePolicy.Preferred, QSizePolicy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.contraseALabel_2.sizePolicy().hasHeightForWidth())
        self.contraseALabel_2.setSizePolicy(sizePolicy)
        self.contraseALabel_2.setObjectName("contraseALabel_2")
        self.formLayout_3.setWidget(1, QFormLayout.LabelRole, self.contraseALabel_2)
        self.contraseALineEdit_2 = QLineEdit(self.gridLayoutWidget_2)
        self.contraseALineEdit_2.setEchoMode(QLineEdit.Password)
        self.contraseALineEdit_2.setObjectName("contraseALineEdit_2")
        self.formLayout_3.setWidget(1, QFormLayout.FieldRole, self.contraseALineEdit_2)
        self.gridLayout_8.addLayout(self.formLayout_3, 0, 1, 1, 1)
        self.gridLayout_7.addLayout(self.gridLayout_8, 4, 1, 1, 1)
        spacerItem7 = QSpacerItem(50, 10, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_7.addItem(spacerItem7, 3, 2, 1, 1)
        spacerItem8 = QSpacerItem(50, 10, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_7.addItem(spacerItem8, 3, 0, 1, 1)
        spacerItem9 = QSpacerItem(50, 10, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_7.addItem(spacerItem9, 0, 2, 1, 1)
        spacerItem10 = QSpacerItem(50, 10, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_7.addItem(spacerItem10, 0, 0, 1, 1)
        self.verticalLayout.addLayout(self.gridLayout_7)
        self.gridLayout_6.addLayout(self.verticalLayout, 0, 0, 1, 1)
        self.gridLayout_10 = QGridLayout()
        self.gridLayout_10.setObjectName("gridLayout_10")
        self.pushButton_2 = QPushButton(self.gridLayoutWidget_2)
        self.pushButton_2.setCheckable(False)
        self.pushButton_2.setAutoRepeat(False)
        self.pushButton_2.setAutoExclusive(False)
        self.pushButton_2.setAutoDefault(False)
        self.pushButton_2.setDefault(True)
        self.pushButton_2.setFlat(False)
        self.pushButton_2.setObjectName("pushButton_2")
        self.gridLayout_10.addWidget(self.pushButton_2, 1, 1, 1, 1)
        spacerItem11 = QSpacerItem(40, 20, QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.gridLayout_10.addItem(spacerItem11, 1, 2, 1, 1)
        spacerItem12 = QSpacerItem(40, 20, QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.gridLayout_10.addItem(spacerItem12, 1, 0, 1, 1)
        self.gridLayout_6.addLayout(self.gridLayout_10, 2, 0, 1, 1)
        self.gridLayout_9 = QGridLayout()
        self.gridLayout_9.setObjectName("gridLayout_9")
        self.line_4 = QFrame(self.gridLayoutWidget_2)
        self.line_4.setFrameShape(QFrame.HLine)
        self.line_4.setFrameShadow(QFrame.Sunken)
        self.line_4.setObjectName("line_4")
        self.gridLayout_9.addWidget(self.line_4, 0, 1, 1, 1)
        spacerItem13 = QSpacerItem(50, 10, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_9.addItem(spacerItem13, 0, 2, 1, 1)
        spacerItem14 = QSpacerItem(50, 10, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_9.addItem(spacerItem14, 0, 0, 1, 1)
        self.gridLayout_6.addLayout(self.gridLayout_9, 1, 0, 1, 1)
        
        # Lista de descarga
        self.list_descarga = [1]
        # Click para conectar a método login
        self.pushButton_2.clicked.connect(self.login)
        # Avance progreso de la descarga
        self.progreso = -1
        # Resultado descarga
        self.result = ""

        # Dataframe que tiene base original
        self.df_or = pd.DataFrame()
        self.retranslateUi(Dialog)
        QMetaObject.connectSlotsByName(Dialog)

    def retranslateUi(self, Dialog):
        _translate = QCoreApplication.translate
        Dialog.setWindowTitle(_translate("Dialog", "Ingreso Validador CMD"))
        self.label_3.setText(_translate("Dialog", "<html><head/><body><p><img src=\":/login/logo_cmd3.png\"/></p></body></html>"))
        self.label_4.setText(_translate("Dialog", "<html><head/><body><p align=\"center\"><span style=\" font-size:12pt; font-weight:600;\">Ingreso Validador CMD</span></p></body></html>"))
        self.usuarioLabel_2.setText(_translate("Dialog", "Usuario:"))
        self.contraseALabel_2.setText(_translate("Dialog", "Contraseña:"))
        self.pushButton_2.setText(_translate("Dialog", "Ingresar"))

    def success_login(self):
        self.resize(391, 641)
        self.gridLayoutWidget_2.setGeometry(QRect(10, 10, 371, 611))

        self.pushButton_2.deleteLater()

        # Layout Activar Errores de Codificación + Etiqueta de "Inicia sesión como..."
        self.gridLayout_12 = QGridLayout()
        self.gridLayout_12.setObjectName("gridLayout_12")
        spacerItem29 = QSpacerItem(50, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_12.addItem(spacerItem29, 2, 2, 1, 1)
        spacerItem30 = QSpacerItem(50, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_12.addItem(spacerItem30, 2, 0, 1, 1)
        spacerItem31 = QSpacerItem(50, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_12.addItem(spacerItem31, 0, 2, 1, 1)
        self.label_9 = QLabel(self.gridLayoutWidget_2)
        sizePolicy = QSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.label_9.sizePolicy().hasHeightForWidth())
        self.label_9.setSizePolicy(sizePolicy)
        self.label_9.setAlignment(Qt.AlignCenter)
        self.label_9.setObjectName("label_9")
        self.gridLayout_12.addWidget(self.label_9, 0, 1, 1, 1)
        spacerItem32 = QSpacerItem(50, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_12.addItem(spacerItem32, 0, 0, 1, 1)
        spacerItem33 = QSpacerItem(20, 10, QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.gridLayout_12.addItem(spacerItem33, 1, 1, 1, 1)
        self.groupBox = QGroupBox(self.gridLayoutWidget_2)
        sizePolicy = QSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.groupBox.sizePolicy().hasHeightForWidth())
        self.groupBox.setSizePolicy(sizePolicy)
        self.groupBox.setMinimumSize(QSize(0, 52))
        self.groupBox.setObjectName("groupBox")
        self.gridLayoutWidget = QWidget(self.groupBox)
        self.gridLayoutWidget.setGeometry(QRect(20, 20, 231, 21))
        self.gridLayoutWidget.setObjectName("gridLayoutWidget")
        self.gridLayout = QGridLayout(self.gridLayoutWidget)
        self.gridLayout.setContentsMargins(0, 0, 0, 0)
        self.gridLayout.setObjectName("gridLayout")
        self.checkBox = QCheckBox(self.gridLayoutWidget)
        sizePolicy = QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.checkBox.sizePolicy().hasHeightForWidth())
        self.checkBox.setSizePolicy(sizePolicy)
        self.checkBox.setText("")
        self.checkBox.setObjectName("checkBox")
        self.gridLayout.addWidget(self.checkBox, 0, 0, 1, 1)
        self.label = QLabel(self.gridLayoutWidget)
        self.label.setObjectName("label")
        self.gridLayout.addWidget(self.label, 0, 1, 1, 1)
        self.gridLayout_12.addWidget(self.groupBox, 2, 1, 1, 1)
        spacerItem34 = QSpacerItem(50, 10, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_12.addItem(spacerItem34, 1, 2, 1, 1)
        spacerItem35 = QSpacerItem(50, 10, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_12.addItem(spacerItem35, 1, 0, 1, 1)
        self.gridLayout_6.addLayout(self.gridLayout_12, 3, 0, 1, 1)

        self.checkBox.stateChanged.connect(self.clickBox)

        # Layout Opciones de descarga
        self.gridLayout_13 = QGridLayout()
        self.gridLayout_13.setObjectName("gridLayout_13")
        spacerItem19 = QSpacerItem(50, 10, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_13.addItem(spacerItem19, 2, 2, 1, 1)
        spacerItem20 = QSpacerItem(50, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_13.addItem(spacerItem20, 1, 2, 1, 1)
        spacerItem21 = QSpacerItem(50, 10, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_13.addItem(spacerItem21, 2, 0, 1, 1)
        spacerItem22 = QSpacerItem(20, 10, QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.gridLayout_13.addItem(spacerItem22, 0, 1, 1, 1)
        spacerItem23 = QSpacerItem(20, 10, QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.gridLayout_13.addItem(spacerItem23, 2, 1, 1, 1)
        spacerItem24 = QSpacerItem(50, 10, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_13.addItem(spacerItem24, 0, 0, 1, 1)
        spacerItem25 = QSpacerItem(50, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_13.addItem(spacerItem25, 1, 0, 1, 1)
        self.groupBox_2 = QGroupBox(self.gridLayoutWidget_2)
        sizePolicy = QSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.groupBox_2.sizePolicy().hasHeightForWidth())
        self.groupBox_2.setSizePolicy(sizePolicy)
        self.groupBox_2.setMinimumSize(QSize(0, 72))
        self.groupBox_2.setObjectName("groupBox_2")
        self.gridLayoutWidget_3 = QWidget(self.groupBox_2)
        self.gridLayoutWidget_3.setGeometry(QRect(20, 20, 231, 43))
        self.gridLayoutWidget_3.setObjectName("gridLayoutWidget_3")
        self.gridLayout_2 = QGridLayout(self.gridLayoutWidget_3)
        self.gridLayout_2.setContentsMargins(0, 0, 0, 0)
        self.gridLayout_2.setObjectName("gridLayout_2")
        self.label_2 = QLabel(self.gridLayoutWidget_3)
        self.label_2.setObjectName("label_2")
        self.gridLayout_2.addWidget(self.label_2, 0, 1, 1, 1)
        self.checkBox_4 = QCheckBox(self.gridLayoutWidget_3)
        sizePolicy = QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.checkBox_4.sizePolicy().hasHeightForWidth())
        self.checkBox_4.setSizePolicy(sizePolicy)
        self.checkBox_4.setMaximumSize(QSize(16777215, 16777215))
        self.checkBox_4.setText("")
        self.checkBox_4.setObjectName("checkBox_4")
        self.gridLayout_2.addWidget(self.checkBox_4, 1, 2, 1, 1)
        self.label_7 = QLabel(self.gridLayoutWidget_3)
        self.label_7.setObjectName("label_7")
        self.gridLayout_2.addWidget(self.label_7, 1, 3, 1, 1)
        self.checkBox_5 = QCheckBox(self.gridLayoutWidget_3)
        sizePolicy = QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.checkBox_5.sizePolicy().hasHeightForWidth())
        self.checkBox_5.setSizePolicy(sizePolicy)
        self.checkBox_5.setMaximumSize(QSize(16777215, 16777215))
        self.checkBox_5.setText("")
        self.checkBox_5.setObjectName("checkBox_5")
        self.gridLayout_2.addWidget(self.checkBox_5, 1, 0, 1, 1)
        self.label_6 = QLabel(self.gridLayoutWidget_3)
        self.label_6.setObjectName("label_6")
        self.gridLayout_2.addWidget(self.label_6, 1, 1, 1, 1)
        self.label_5 = QLabel(self.gridLayoutWidget_3)
        self.label_5.setObjectName("label_5")
        self.gridLayout_2.addWidget(self.label_5, 0, 3, 1, 1)
        self.checkBox_3 = QCheckBox(self.gridLayoutWidget_3)
        sizePolicy = QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.checkBox_3.sizePolicy().hasHeightForWidth())
        self.checkBox_3.setSizePolicy(sizePolicy)
        self.checkBox_3.setMaximumSize(QSize(16777215, 16777215))
        self.checkBox_3.setText("")
        self.checkBox_3.setObjectName("checkBox_3")
        self.gridLayout_2.addWidget(self.checkBox_3, 0, 2, 1, 1)
        self.checkBox_2 = QCheckBox(self.gridLayoutWidget_3)
        sizePolicy = QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.checkBox_2.sizePolicy().hasHeightForWidth())
        self.checkBox_2.setSizePolicy(sizePolicy)
        self.checkBox_2.setMinimumSize(QSize(0, 0))
        self.checkBox_2.setMaximumSize(QSize(16777215, 16777215))
        self.checkBox_2.setText("")
        self.checkBox_2.setObjectName("checkBox_2")
        self.gridLayout_2.addWidget(self.checkBox_2, 0, 0, 1, 1)
        self.gridLayout_13.addWidget(self.groupBox_2, 1, 1, 1, 1)
        spacerItem26 = QSpacerItem(50, 10, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_13.addItem(spacerItem26, 0, 2, 1, 1)
        self.gridLayout_6.addLayout(self.gridLayout_13, 4, 0, 1, 1)

        self.checkBox_2.setChecked(True)
        self.checkBox_2.stateChanged.connect(self.clickBox_or)
        self.checkBox_5.stateChanged.connect(self.clickBox_sh)
        self.checkBox_3.stateChanged.connect(self.clickBox_pr)
        self.checkBox_4.stateChanged.connect(self.clickBox_ct)

        # Layout botón "Descargar"
        self.gridLayout_15 = QGridLayout()
        self.gridLayout_15.setObjectName("gridLayout_15")
        self.pushButton_4 = QPushButton(self.gridLayoutWidget_2)
        self.pushButton_4.setFocusPolicy(Qt.WheelFocus)
        self.pushButton_4.setContextMenuPolicy(Qt.DefaultContextMenu)
        self.pushButton_4.setAcceptDrops(False)
        self.pushButton_4.setLayoutDirection(Qt.LeftToRight)
        self.pushButton_4.setAutoFillBackground(False)
        self.pushButton_4.setStyleSheet("border-bottom-color: rgb(255, 255, 255);")
        self.pushButton_4.setCheckable(False)
        self.pushButton_4.setAutoRepeat(False)
        self.pushButton_4.setAutoExclusive(False)
        self.pushButton_4.setAutoDefault(False)
        self.pushButton_4.setDefault(True)
        self.pushButton_4.setFlat(False)
        self.pushButton_4.setObjectName("pushButton_4")
        self.gridLayout_15.addWidget(self.pushButton_4, 1, 1, 1, 1)
        spacerItem27 = QSpacerItem(40, 20, QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.gridLayout_15.addItem(spacerItem27, 1, 2, 1, 1)
        spacerItem28 = QSpacerItem(40, 20, QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.gridLayout_15.addItem(spacerItem28, 1, 0, 1, 1)
        self.gridLayout_6.addLayout(self.gridLayout_15, 5, 0, 1, 1)

        self.pushButton_4.clicked.connect(self.descarga)

        # Translate
        _translate = QCoreApplication.translate
        self.label_9.setText(_translate("Dialog", f"<html><head/><body><p>Has iniciado sesión como <span style=' font-weight:600;'>{self.parent().username}</span></p></body></html>"))
        self.groupBox.setTitle(_translate("Dialog", "Opciones"))
        self.label.setText(_translate("Dialog", "Errores de codificación"))
        self.groupBox_2.setTitle(_translate("Dialog", "Descarga"))
        self.label_2.setText(_translate("Dialog", "Originales"))
        self.label_7.setText(_translate("Dialog", "Controles"))
        self.label_6.setText(_translate("Dialog", "Segundo Hogar"))
        self.label_5.setText(_translate("Dialog", "Pruebas"))
        self.pushButton_4.setText(_translate("Dialog", "Descargar"))

    def download_initiated(self):
        self.resize(391, 671)
        self.gridLayoutWidget_2.setGeometry(QRect(10, 10, 371, 631))

        # Layout barra de progreso de descarga
        self.gridLayout_11 = QGridLayout()
        self.gridLayout_11.setObjectName("gridLayout_11")
        spacerItem15 = QSpacerItem(50, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_11.addItem(spacerItem15, 0, 0, 1, 1)
        spacerItem16 = QSpacerItem(50, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_11.addItem(spacerItem16, 0, 2, 1, 1)
        self.progressBar = QProgressBar(self.gridLayoutWidget_2)
        self.progressBar.setAutoFillBackground(False)
        self.progressBar.setProperty("value", 24)
        self.progressBar.setAlignment(Qt.AlignCenter)
        self.progressBar.setTextVisible(True)
        self.progressBar.setOrientation(Qt.Horizontal)
        self.progressBar.setInvertedAppearance(False)
        self.progressBar.setTextDirection(QProgressBar.TopToBottom)
        self.progressBar.setObjectName("progressBar")
        self.gridLayout_11.addWidget(self.progressBar, 0, 1, 1, 1)
        self.label_8 = QLabel(self.gridLayoutWidget_2)
        sizePolicy = QSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.label_8.sizePolicy().hasHeightForWidth())
        self.label_8.setSizePolicy(sizePolicy)
        self.label_8.setAlignment(Qt.AlignCenter)
        self.label_8.setObjectName("label_8")
        self.gridLayout_11.addWidget(self.label_8, 1, 1, 1, 1)
        spacerItem17 = QSpacerItem(50, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_11.addItem(spacerItem17, 1, 0, 1, 1)
        spacerItem18 = QSpacerItem(50, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.gridLayout_11.addItem(spacerItem18, 1, 2, 1, 1)
        self.gridLayout_6.addLayout(self.gridLayout_11, 6, 0, 1, 1)

        # Translate
        _translate = QCoreApplication.translate
        self.label_8.setText(_translate("Dialog", "Descargando..."))

    def login(self):

        self.parent().username = self.usuarioLineEdit_2.text()
        self.password = self.contraseALineEdit_2.text()

        print(self.parent().username)
        print(self.password)
        self.data_login = Usuarios().login_user(username=self.parent().username, password=self.password)
        print(self.data_login)

        if self.data_login:
            
            self.success_login()
        
        if not self.data_login:
            QMessageBox.warning(self, 'Credenciales erróneas', 'Las credenciales ingresadas no corresponden a ninguna combinación usuario-contraseña presente en el sistema. Por favor verifique los datos ingresados.', QMessageBox.Ok, QMessageBox.Ok)

    #########################################################################
    # Función para actualizar estado de input revisar codificación
    #########################################################################

    def clickBox(self, state):

        if state == Qt.Checked:
            self.parent().RevCod1 = 1
            print("El codigo en clickbox es "+str(self.parent().RevCod1))
        else:
            self.parent().RevCod1 = 0
            print("El codigo en clickbox es "+str(self.parent().RevCod1))

    #########################################################################
    # Función para marcar encuestas a descargar
    #########################################################################

    def clickBox_or(self, state):
        if state == Qt.Checked:
            try:
                self.list_descarga.remove(1)
            except Exception as exception:
                pass
            self.list_descarga = self.list_descarga + [1]
            print(self.list_descarga)
        else:
            try:
                self.list_descarga.remove(1)
            except Exception as exception:
                pass
            print(self.list_descarga)

    def clickBox_sh(self, state):
        if state == Qt.Checked:
            try:
                self.list_descarga.remove(2)
            except Exception as exception:
                pass
            self.list_descarga = self.list_descarga + [2]
            print(self.list_descarga)
        else:
            try:
                self.list_descarga.remove(2)
            except Exception as exception:
                pass
            print(self.list_descarga)

    def clickBox_pr(self, state):
        if state == Qt.Checked:
            try:
                self.list_descarga.remove(-1)
            except Exception as exception:
                pass
            self.list_descarga = self.list_descarga + [-1]
            print(self.list_descarga)
        else:
            try:
                self.list_descarga.remove(-1)
            except Exception as exception:
                pass
            print(self.list_descarga)

    def clickBox_ct(self, state):
        if state == Qt.Checked:
            try:
                self.list_descarga.remove(4)
            except Exception as exception:
                pass
            self.list_descarga = self.list_descarga + [4]
            print(self.list_descarga)
        else:
            try:
                self.list_descarga.remove(4)
            except Exception as exception:
                pass
            print(self.list_descarga)

    def onCountChanged(self, value):

        self.progreso = value
        self.progressBar.setValue(value)
        if value == 100:
            self.progressBar.hide()
            self.progreso = -1

    ########################################################################
    # Funcion para descargar
    ########################################################################

    def descarga(self):
        if self.progreso == -1:
            
            self.download_initiated()
            self.setEnabled(False)
            QApplication.setOverrideCursor(Qt.WaitCursor)

            self.progreso = 0
            self.progressBar.show()
            self.progressBar.setValue(0)

            self.calc = External(self)
            self.calc.countChanged.connect(self.onCountChanged)
            self.calc.start()

            self.calc.result.connect(self.RecResult)
            self.calc.df.connect(self.Recdf)

    ########################################################################
    # Funcion para conectar
    ########################################################################

    def RecResult(self, value):
        self.result = value
        
    def Recdf(self, value):
        self.parent().df = value
        # Pasa variables numericas a formato float (así normaliza tratamiento)

        for column in self.parent().df:
            if self.parent().df[column].dtype == "int64":
                # print(column)
                self.parent().df[column] = self.parent().df[column].astype('float64')

        f = tempfileTemporaryDirectory()
        self.parent().df.to_csv(ospathjoin(f.name, "df.csv"), index=False)
        self.parent().df = pd.read_csv(ospathjoin(f.name, "df.csv"))

        try:
            shutilrmtree(f.name)
        except Exception as exception:
            pass

        if self.result == 'Completado':

            QApplication.restoreOverrideCursor()
            # Actualiza estado de errores
            self.parent().df_Errores = External(self).UpdListError2(self.parent().df, rev_cod=self.parent().RevCod1, key=None)

            # print("El código en recdf es" + str(self.RevCod1))
            # df_Errores.to_csv("C:\\Users\\usuario\\Downloads\\df_Errores.csv")
            # self.parent().df.to_csv("C:\\Users\\usuario\\Downloads\\df.csv")

            # df_Errores = pd.read_csv("C:\\Users\\usuario\\Downloads\\df_Errores.csv")
            # df = pd.read_csv("C:\\Users\\usuario\\Downloads\\df.csv")
            le = self.parent().df_Errores['interview__key'].drop_duplicates()

            self.parent().df = self.parent().df.merge(le, on='interview__key', indicator='exists', how='outer')
            self.parent().df = self.parent().df.loc[self.parent().df.exists != "right_only", :]
            # self.parent().df.loc[(self.parent().df._merge!="both"),['estado']] =   0 # Encuestas que no pegaron, no tienen errores entonces: 1) Silenciar mantener historial cambios, 2) Descomentar para reinicar
            self.parent().df.loc[(self.parent().df.exists == "both") & (self.parent().df.estado == 0), ['estado']] = 1  # Encuestas que  pegaron,  tienen errores
            self.parent().df.loc[(self.parent().df.exists == "both") & (self.parent().df.estado == 1), ['estado']] = 1  # Encuestas que  pegaron,  tienen errores
            self.parent().df.loc[(self.parent().df.exists == "both") & (self.parent().df.estado == 2), ['estado']] = 1  # Encuestas que  pegaron,  tienen errores
            self.parent().df.loc[(self.parent().df.exists == "both") & (self.parent().df.estado == 3), ['estado']] = 3  # Encuestas que  pegaron forzadas,  dejarlas forzadas
            self.parent().df = self.parent().df.drop(['exists'], axis=1)

            # Numero de observaciones
            n_DataFull = self.parent().df['interview__key'].nunique()
            # Numero errores
            n_ErrorValidar = self.parent().df.loc[self.parent().df.estado == 1, 'interview__key'].nunique()
            # print(n_ErrorValidar)
            MensajeConexion = "Conexión Exitosa, {x} registros unicos y {y} registros por validar".format(x=n_DataFull, y=n_ErrorValidar)
            self.progressBar.hide()
            self.label_8.hide()
            self.resize(391, 641)
            self.gridLayoutWidget_2.setGeometry(QRect(10, 10, 371, 611))

        if self.result != 'Completado':
            MensajeConexion = self.result
            self.setEnabled(True)

        # Mensaje de resultado
        msgBox = QMessageBox()
        msgBox.setIcon(QMessageBox.Information)
        msgBox.setText(MensajeConexion)
        msgBox.setWindowTitle("Conexión")
        msgBox.setStandardButtons(QMessageBox.Ok)
        msgBox.exec()
        self.close()

        if self.data_login[0][1] == 'admin':
            self.parent().admin_features()
        self.parent().show()
        self.parent().habilita_botones()
        self.parent().UpdateErrorsEncuesta()


if __name__ == "__main__":
    import sys
    app = QApplication(sys.argv)
    w = MainWindow()
    app.exec_()
