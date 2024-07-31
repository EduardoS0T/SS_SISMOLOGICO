use strict;
use warnings;
use File::Basename;
use File::Copy;
use File::Spec;
use File::Path qw(make_path);
use Fcntl qw(:flock SEEK_END);
use POSIX qw(strftime mktime);
use LWP::UserAgent;
use URI::Escape;

my $base_dir = "/home/eduardo/Documentos/ServicioSocial";
my $carpeta_entrada = "$base_dir/Entradas";
my $carpeta_salida = "$base_dir/Revisados";
my $carpeta_rechazados = "$base_dir/Rechazados";
my $carpeta_archivos = "$base_dir/ArchivosGenerados";
my $carpeta_archivos_unico = "$base_dir/ArchivosGeneradosUnico";
my $carpeta_reportes = "$base_dir/Reportes";
my $carpeta_mapas = "$base_dir/Mapas";

foreach my $dir ($carpeta_entrada, $carpeta_salida, $carpeta_rechazados, $carpeta_archivos, $carpeta_archivos_unico, $carpeta_reportes, $carpeta_mapas) {
    unless (-e $dir && -d _) {
        make_path($dir) or die "No se pudo crear el directorio $dir: $!";
    }
}

sub nombre_archivo_csv {
    my ($carpeta) = @_;
    my ($segundos, $minutos, $horas, $dia, $mes, $anio) = localtime(time);
    $anio += 1900;
    $mes += 1;
    return sprintf("%s/datos_extraidos_%04d%02d%02d_%02d%02d%02d.csv", $carpeta, $anio, $mes, $dia, $horas, $minutos, $segundos);
}

sub abrir_archivo_csv {
    my ($archivo_csv) = @_;
    open(my $csv, '>', $archivo_csv) or die "No se pudo crear el archivo CSV $archivo_csv: $!";
    print $csv "fecha,hora,latitud,longitud,profundidad,magnitud,epicentro,fechaUTC,horaUTC\n";
    flock($csv, LOCK_EX) or die "No se pudo bloquear el archivo CSV $archivo_csv: $!";
    return $csv;
}

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

sub generar_mapa {
    my ($latitud, $longitud, $nombre_mapa) = @_;
    my $api_key = 'AIzaSyCRoRdEBeMZUfx_kSjmB-Dgezk2jYWh7bQ';
    my $size = '600x400';
    my $zoom = 4.0;
    my $center = '23.6345,-102.5528';
    my $marker = uri_escape("$latitud,$longitud");
    my $maptype = 'hybrid';
    my $url = "https://maps.googleapis.com/maps/api/staticmap?center=$center&zoom=$zoom&size=$size&maptype=$maptype&markers=color:red%7Clabel:S%7C$marker&key=$api_key";
    my $ua = LWP::UserAgent->new;
    my $response = $ua->get($url);
    if ($response->is_success) {
        open my $fh, '>', $nombre_mapa or die "No se pudo crear el archivo de mapa $nombre_mapa: $!";
        binmode $fh;
        print $fh $response->content;
        close $fh;
    } else {
        warn "No se pudo obtener el mapa: " . $response->status_line;
    }
}

while (1) {
    my $archivo_csv;
    my $csv;
    my $nuevo_dato = 0;
    my $num_archivos_procesados = 0;
    my ($magnitud_unico, $epicentro_unico, $fecha_unico, $hora_unico, $latitud_unico, $longitud_unico, $profundidad_unico);

    my @archivos = glob("$carpeta_entrada/*");
    
    foreach my $archivo (@archivos) {
        next if basename($archivo) =~ /^\./;
        next if $archivo eq $0;

        my $contenido = '';
        if (open(my $fh, '<', $archivo)) {
            $contenido = do { local $/; <$fh> };
            close($fh);
        } else {
            warn "No se pudo abrir el archivo '$archivo': $!";
            next;
        }
        
        if ($contenido =~ /SISMO\s+Magnitud\s+(\d+\.\d+)\s+Loc\.\s+(\d+)\s+km\s+al\s+(\w+)\s+de\s+(.+?)\s+(\d{2}\/\d{2}\/\d{2})\s+(\d{2}:\d{2}:\d{2})\s+Lat\s+([\d.-]+)\s+Lon\s+([\d.-]+)\s+Pf\s+(\d+(?:\.\d)?)\s+km/) {
            my ($magnitud, $distancia, $direccion, $ubicacion, $fecha, $hora, $latitud, $longitud, $profundidad) = ($1, $2, $3, $4, $5, $6, $7, $8, $9);

            $fecha =~ s#(\d{2})/(\d{2})/(\d{2})#20$3-$2-$1#;
            my ($anio, $mes, $dia) = split(/-/, $fecha);
            my ($hora_h, $hora_m, $hora_s) = split(/:/, $hora);
            $hora_h += 6;
            if ($hora_h >= 24) {
                $hora_h -= 24;
                $dia++;
                my @dias_por_mes = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31);
                if ($anio % 4 == 0 && ($anio % 100 != 0 || $anio % 400 == 0)) {
                    $dias_por_mes[1] = 29;
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
            my $fecha_utc = sprintf("%04d-%02d-%02d", $anio, $mes, $dia);
            my $hora_utc = sprintf("%02d:%02d:%02d", $hora_h, $hora_m, $hora_s);

            $num_archivos_procesados++;
            if (!defined $archivo_csv) {
                $archivo_csv = nombre_archivo_csv($carpeta_archivos);
                $csv = abrir_archivo_csv($archivo_csv);
            }

            print $csv "$fecha,$hora,$latitud,$longitud,$profundidad,$magnitud,$ubicacion,$fecha_utc,$hora_utc\n";
            $nuevo_dato = 1;

            if ($num_archivos_procesados == 1) {
                $magnitud_unico = $magnitud;
                $epicentro_unico = "$distancia km al $direccion de $ubicacion";
                $fecha_unico = $fecha;  # Usar la fecha original
                $hora_unico = $hora;    # Usar la hora original
                $latitud_unico = $latitud;
                $longitud_unico = $longitud;
                $profundidad_unico = $profundidad;
            }
            move($archivo, "$carpeta_salida/" . basename($archivo)) or warn "No se pudo mover el archivo $archivo a $carpeta_salida: $!";
        } else {
            move($archivo, "$carpeta_rechazados/" . basename($archivo)) or warn "No se pudo mover el archivo $archivo a $carpeta_rechazados: $!";
        }
    }

    if (defined $csv) {
        flock($csv, LOCK_UN) or warn "No se pudo desbloquear el archivo CSV $archivo_csv: $!";
        close($csv) or warn "No se pudo cerrar el archivo CSV $archivo_csv: $!";

        if ($num_archivos_procesados == 1) {
            if ($magnitud_unico >= 4.0) {
                my $mensaje_reporte = generar_reporte($magnitud_unico, $epicentro_unico, $fecha_unico, $hora_unico, $latitud_unico, $longitud_unico, $profundidad_unico);
                guardar_reporte($mensaje_reporte);

                my $nombre_mapa = "$carpeta_mapas/mapa_sismo_$fecha_unico\_$hora_unico.png";
                generar_mapa($latitud_unico, $longitud_unico, $nombre_mapa);
                
                move($archivo_csv, "$carpeta_archivos_unico/" . basename($archivo_csv)) or warn "No se pudo mover el archivo CSV $archivo_csv a $carpeta_archivos_unico: $!";
            } else {
                move($archivo_csv, "$carpeta_archivos/" . basename($archivo_csv)) or warn "No se pudo mover el archivo CSV $archivo_csv a $carpeta_archivos: $!";
            }
        } else {
            move($archivo_csv, "$carpeta_archivos/" . basename($archivo_csv)) or warn "No se pudo mover el archivo CSV $archivo_csv a $carpeta_archivos: $!";
        }
    }

    sleep 3;
}

