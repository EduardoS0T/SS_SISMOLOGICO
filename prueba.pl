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
my $ultima_ejecucion = "$base_dir/ultima_ejecucion.txt";

# Verificar y crear directorios si no existen
foreach my $dir ($carpeta_entrada, $carpeta_salida, $carpeta_rechazados, $carpeta_archivos) {
    unless (-e $dir && -d _) {
        make_path($dir) or die "No se pudo crear el directorio $dir: $!";
    }
}

# Obtener timestamp de la última ejecución exitosa
sub obtener_ultimo_timestamp {
    if (-e $ultima_ejecucion) {
        open my $fh, '<', $ultima_ejecucion or die "No se pudo abrir el archivo '$ultima_ejecucion': $!";
        my $timestamp = <$fh>;
        close $fh;
        chomp $timestamp;
        return $timestamp;
    } else {
        return 0; # Timestamp cero si el archivo no existe
    }
}

# Guardar timestamp de la ejecución actual
sub guardar_timestamp_actual {
    open my $fh, '>', $ultima_ejecucion or die "No se pudo escribir en el archivo '$ultima_ejecucion': $!";
    print $fh time;
    close $fh;
}

# Función para obtener el nombre de archivo con marca de tiempo
sub nombre_archivo_csv {
    my ($segundos, $minutos, $horas, $dia, $mes, $anio) = localtime(time);
    $anio += 1900;  # Ajuste del año
    $mes += 1;      # Ajuste del mes (0..11)
    return sprintf("%s/datos_extraidos_%04d%02d%02d_%02d%02d%02d.csv", $carpeta_archivos, $anio, $mes, $dia, $horas, $minutos, $segundos);
}

# Función para abrir o crear un nuevo archivo CSV con encabezados de columna
sub abrir_archivo_csv {
    my ($archivo_csv) = @_;

    open(my $csv, '>', $archivo_csv) or die "No se pudo crear el archivo CSV $archivo_csv: $!";
    print $csv "fecha,hora,latitud,longitud,profundidad,magnitud,epicentro,fechaUTC,horaUTC\n";
    flock($csv, LOCK_EX) or die "No se pudo bloquear el archivo CSV $archivo_csv: $!";
    return $csv;
}

# Obtener el timestamp de la última ejecución
my $ultimo_timestamp = obtener_ultimo_timestamp();

while (1) {
    my $archivo_csv;
    my $csv;
    my $nuevo_dato = 0;  # Bandera para verificar si hay nuevos datos

    my @archivos = glob("$carpeta_entrada/*");
    
    foreach my $archivo (@archivos) {
        next if basename($archivo) =~ /^\./; # Ignorar archivos ocultos
        next if $archivo eq $0; # Ignorar el archivo del script actual

        my $mod_time = (stat($archivo))[9]; # Obtener tiempo de modificación del archivo
        next if $mod_time <= $ultimo_timestamp; # Ignorar archivos ya procesados

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
                $archivo_csv = nombre_archivo_csv();
                $csv = abrir_archivo_csv($archivo_csv);
            }

            # Escribir los datos en el archivo CSV
            print $csv "$fecha,$hora,$latitud,$longitud,$profundidad,$magnitud,$epicentro,$fechaUTC,$horaUTC\n";

            # Mover el archivo a la carpeta de salida
            move($archivo, $carpeta_salida) or warn "No se pudo mover el archivo '$archivo' a '$carpeta_salida': $!";
            
            $nuevo_dato = 1; # Se encontraron nuevos datos
        } else {
            warn "El archivo '$archivo' no coincide con el formato esperado.\n";
            # Mover el archivo a la carpeta de rechazados
            move($archivo, $carpeta_rechazados) or warn "No se pudo mover el archivo '$archivo' a '$carpeta_rechazados': $!";
        }
    }

    # Cerrar el archivo CSV si se ha creado
    if ($csv) {
        close($csv);
        $csv = undef;
    }

    # Actualizar el timestamp solo si hubo nuevos datos procesados
    guardar_timestamp_actual() if $nuevo_dato;

    sleep(2); # Esperar 900 segundos (15 minutos) antes de volver a monitorear la carpeta
}

