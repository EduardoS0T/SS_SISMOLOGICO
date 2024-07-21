use strict;
use warnings;
use File::Basename;
use File::Copy;
use File::Spec;
use File::Path qw(make_path);
use Fcntl qw(:flock SEEK_END);
use POSIX qw(strftime mktime);

my $base_dir = "/home/eduardo/Documentos/ServicioSocial";
my $carpeta_entrada = "$base_dir/Entradas";
my $carpeta_salida = "$base_dir/Revisados";
my $carpeta_rechazados = "$base_dir/Rechazados";
my $carpeta_archivos = "$base_dir/ArchivosGenerados";
my $carpeta_archivos_unico = "$base_dir/ArchivosGeneradosUnico";  # Nueva carpeta para un solo archivo
my $carpeta_reportes = "$base_dir/Reportes";  # Nueva carpeta para los reportes

# Verificar y crear directorios si no existen
foreach my $dir ($carpeta_entrada, $carpeta_salida, $carpeta_rechazados, $carpeta_archivos, $carpeta_archivos_unico, $carpeta_reportes) {
    unless (-e $dir && -d _) {
        make_path($dir) or die "No se pudo crear el directorio $dir: $!";
    }
}

# Función para obtener el nombre de archivo con marca de tiempo
sub nombre_archivo_csv {
    my ($carpeta) = @_;
    my ($segundos, $minutos, $horas, $dia, $mes, $anio) = localtime(time);
    $anio += 1900;  # Ajuste del año
    $mes += 1;      # Ajuste del mes (0..11)
    return sprintf("%s/datos_extraidos_%04d%02d%02d_%02d%02d%02d.csv", $carpeta, $anio, $mes, $dia, $horas, $minutos, $segundos);
}

# Función para abrir o crear un nuevo archivo CSV con encabezados de columna
sub abrir_archivo_csv {
    my ($archivo_csv) = @_;

    open(my $csv, '>', $archivo_csv) or die "No se pudo crear el archivo CSV $archivo_csv: $!";
    print $csv "fecha,hora,latitud,longitud,profundidad,magnitud,epicentro,fechaUTC,horaUTC\n";
    flock($csv, LOCK_EX) or die "No se pudo bloquear el archivo CSV $archivo_csv: $!";
    return $csv;
}

# Función para generar el mensaje de reporte
sub generar_reporte {
    my ($magnitud, $epicentro, $fecha, $hora, $latitud, $longitud, $profundidad) = @_;
    my $reporte = "SSN REPORTA: SISMO\n";
    $reporte .= "Magnitud: $magnitud\n";
    $reporte .= "Región epicentral: $epicentro\n";
    $reporte .= "Fecha y hora: $fecha, $hora (tiempo del Centro de México)\n";
    $reporte .= "Latitud y longitud: $latitudº, $longitudº\n";
    $reporte .= "Profundidad: $profundidad km\n";
    return $reporte;
}

# Función para guardar el reporte en un archivo
sub guardar_reporte {
    my ($mensaje_reporte) = @_;
    my $archivo_reporte = sprintf("%s/reporte_%04d%02d%02d_%02d%02d%02d.txt",
                                  $carpeta_reportes,
                                  (localtime(time))[5] + 1900,
                                  (localtime(time))[4] + 1,
                                  (localtime(time))[3],
                                  (localtime(time))[2],
                                  (localtime(time))[1],
                                  (localtime(time))[0]);
    open(my $fh, '>', $archivo_reporte) or die "No se pudo crear el archivo de reporte $archivo_reporte: $!";
    print $fh $mensaje_reporte;
    close($fh);
}

while (1) {
    my $archivo_csv;
    my $csv;
    my $nuevo_dato = 0;  # Bandera para verificar si hay nuevos datos
    my $num_archivos_procesados = 0;  # Contador de archivos procesados
    my $magnitud_unico;  # Para almacenar la magnitud del único archivo si aplica
    my $epicentro_unico;  # Para almacenar el epicentro del único archivo si aplica
    my $fecha_unico;     # Para almacenar la fecha del único archivo si aplica
    my $hora_unico;      # Para almacenar la hora del único archivo si aplica
    my $latitud_unico;   # Para almacenar la latitud del único archivo si aplica
    my $longitud_unico;  # Para almacenar la longitud del único archivo si aplica
    my $profundidad_unico; # Para almacenar la profundidad del único archivo si aplica

    my @archivos = glob("$carpeta_entrada/*");
    
    foreach my $archivo (@archivos) {
        next if basename($archivo) =~ /^\./; # Ignorar archivos ocultos
        next if $archivo eq $0; # Ignorar el archivo del script actual

        # Leer el contenido del archivo
        my $contenido = '';
        if (open(my $fh, '<', $archivo)) {
            $contenido = do { local $/; <$fh> };
            close($fh);
        } else {
            warn "No se pudo abrir el archivo '$archivo': $!";
            next;
        }
        
        # Extraer los datos utilizando expresiones regulares
        if ($contenido =~ /SISMO\s+Magnitud\s+(\d+\.\d+)\s+Loc\.\s+(\d+)\s+km\s+al\s+(\w+)\s+de\s+(.+?)\s+(\d{2}\/\d{2}\/\d{2})\s+(\d{2}:\d{2}:\d{2})\s+Lat\s+([\d.-]+)\s+Lon\s+([\d.-]+)\s+Pf\s+(\d+(?:\.\d)?)\s+km/) {
            my ($magnitud, $distancia, $direccion, $ubicacion, $fecha, $hora, $latitud, $longitud, $profundidad) = ($1, $2, $3, $4, $5, $6, $7, $8, $9);

            # Ajustar el formato de la fecha (agregar año completo)
            $fecha =~ s#(\d{2})/(\d{2})/(\d{2})#20$3-$2-$1#;

            # Separar la fecha en componentes
            my ($anio, $mes, $dia) = split(/-/, $fecha);
            my ($hora_h, $hora_m, $hora_s) = split(/:/, $hora);

            # Sumar 6 horas a la hora local
            $hora_h += 6;
            if ($hora_h >= 24) {
                $hora_h -= 24;
                $dia++;

                # Ajustar el día y el mes si es necesario
                my @dias_por_mes = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);
                if ($anio % 4 == 0 && ($anio % 100 != 0 || $anio % 400 == 0)) {
                    $dias_por_mes[1] = 29; # Año bisiesto
                }

                if ($dia > $dias_por_mes[$mes - 1]) {
                    $dia = 1;
                    $mes++;
                    if ($mes > 12) {
                        $mes = 1;
                        $anio++;
                    }
                }
            }

            # Formatear fecha y hora UTC
            my $fechaUTC = sprintf("%04d-%02d-%02d", $anio, $mes, $dia);
            my $horaUTC = sprintf("%02d:%02d:%02d", $hora_h, $hora_m, $hora_s);

            # Formatear los datos
            $profundidad = sprintf("%.1f", $profundidad);
            $magnitud = sprintf("%.1f", $magnitud);
            my $epicentro = "$distancia km al $direccion de $ubicacion";

            # Crear archivo CSV si hay nuevos datos
            unless ($csv) {
                $archivo_csv = nombre_archivo_csv($carpeta_archivos);
                $csv = abrir_archivo_csv($archivo_csv);
            }

            # Escribir los datos en el archivo CSV
            print $csv "$fecha,$hora,$latitud,$longitud,$profundidad,$magnitud,$epicentro,$fechaUTC,$horaUTC\n";

            # Mover el archivo a la carpeta de salida
            move($archivo, $carpeta_salida) or warn "No se pudo mover el archivo '$archivo' a '$carpeta_salida': $!";
            
            $nuevo_dato = 1; # Se encontraron nuevos datos
            $num_archivos_procesados++;
            
            # Si es el único archivo procesado, guardar la información para el reporte
            if ($num_archivos_procesados == 1) {
                $magnitud_unico = $magnitud;
                $epicentro_unico = $epicentro;
                $fecha_unico = $fechaUTC;
                $hora_unico = $horaUTC;
                $latitud_unico = $latitud;
                $longitud_unico = $longitud;
                $profundidad_unico = $profundidad;
            }
        } else {
            warn "El archivo '$archivo' no coincide con el formato esperado.\n";
            # Mover el archivo a la carpeta de rechazados
            move($archivo, $carpeta_rechazados) or warn "No se pudo mover el archivo '$archivo' a '$carpeta_rechazados': $!";
        }
    }

    # Cerrar el archivo CSV si se ha creado
    if ($csv) {
        close($csv);
        # Si se procesó solo un archivo y la magnitud es >= 4.0, generar reporte
        if ($num_archivos_procesados == 1 && $magnitud_unico >= 4.0) {
            my $mensaje_reporte = generar_reporte($magnitud_unico, $epicentro_unico, $fecha_unico, $hora_unico, $latitud_unico, $longitud_unico, $profundidad_unico);
            guardar_reporte($mensaje_reporte);
        }
        # Mover el archivo CSV a la carpeta de archivos únicos
        if ($num_archivos_procesados == 1) {
            my $archivo_csv_unico = nombre_archivo_csv($carpeta_archivos_unico);
            move($archivo_csv, $archivo_csv_unico) or warn "No se pudo mover el archivo '$archivo_csv' a '$archivo_csv_unico': $!";
        }
        $csv = undef;
    }

    sleep(2); # Esperar 2 segundos antes de volver a monitorear la carpeta
}

