#include <stdio.h>
#include <sys/wait.h> 
#include <stdlib.h>
#include <pthread.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#define CHUNK_SIZE 4096 // Tamaño del chunk para lectura por partes

pthread_mutex_t lock;  // Mutex para proteger las secciones críticas
int file_count = 0;    // Contador de archivos procesados

// Estructura de argumentos para pasar a cada hilo
typedef struct {
    char *file_data;
    size_t file_size;
    int thread_id;
} ThreadArgs;

// Función para leer y procesar un chunk del archivo
void *read_chunk(void *args) {
    ThreadArgs *thread_args = (ThreadArgs *)args;
    size_t chunk_size = CHUNK_SIZE;
    char *data = thread_args->file_data;
    int id = thread_args->thread_id;

    printf("Hilo %d procesando chunk de tamaño %lu\n", id, chunk_size);

    // Aquí procesarías cada chunk (ej., contar registros, analizar datos)
    // Para este ejemplo, solo mostramos el uso de un hilo para leer

    pthread_exit(NULL);
}

// Función para cargar el archivo en memoria usando mmap y leer en chunks
void load_file_in_memory(const char *filename) {
    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        perror("Error abriendo el archivo");
        exit(EXIT_FAILURE);
    }

    // Obtener el tamaño del archivo
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        perror("Error obteniendo el tamaño del archivo");
        close(fd);
        exit(EXIT_FAILURE);
    }
    size_t file_size = sb.st_size;

    // Mapear el archivo en memoria
    char *file_in_memory = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (file_in_memory == MAP_FAILED) {
        perror("Error en mmap");
        close(fd);
        exit(EXIT_FAILURE);
    }

    close(fd);  // Ya no necesitamos el descriptor de archivo

    // Crear hilos para leer el archivo por chunks
    int num_threads = file_size / CHUNK_SIZE;
    if (file_size % CHUNK_SIZE != 0) num_threads++;

    pthread_t threads[num_threads];
    ThreadArgs thread_args[num_threads];

    for (int i = 0; i < num_threads; i++) {
        thread_args[i].file_data = file_in_memory + (i * CHUNK_SIZE);
        thread_args[i].file_size = (i == num_threads - 1) ? file_size % CHUNK_SIZE : CHUNK_SIZE;
        thread_args[i].thread_id = i;

        pthread_create(&threads[i], NULL, read_chunk, (void *)&thread_args[i]);
    }

    // Esperar a que todos los hilos terminen
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    // Liberar memoria mapeada
    munmap(file_in_memory, file_size);

    pthread_mutex_lock(&lock);
    file_count++;
    pthread_mutex_unlock(&lock);
}

// Función para listar y cargar todos los archivos CSV en un directorio
void load_files_from_directory(const char *directory, int mode) {
    DIR *dir;
    struct dirent *entry;

    if ((dir = opendir(directory)) == NULL) {
        perror("Error abriendo el directorio");
        exit(EXIT_FAILURE);
    }

    while ((entry = readdir(dir)) != NULL) {
        if (strstr(entry->d_name, ".csv") != NULL) {
            char filepath[1024];
            snprintf(filepath, sizeof(filepath), "%s/%s", directory, entry->d_name);

            if (mode == 0) {
                // Modo secuencial: leer un archivo a la vez en un solo núcleo
                load_file_in_memory(filepath);
            } else if (mode == 1) {
                // Modo -s: un hilo por archivo en el mismo proceso
                pthread_t thread;
                pthread_create(&thread, NULL, (void *(*)(void *))load_file_in_memory, (void *)filepath);
                pthread_join(thread, NULL);
            } else if (mode == 2) {
                // Modo -m: proceso por archivo, con hilos leyendo por chunks
                if (fork() == 0) {
                    load_file_in_memory(filepath);
                    exit(EXIT_SUCCESS);
                }
            }
        }
    }

    closedir(dir);

    if (mode == 2) {
        while (wait(NULL) > 0); // Esperar a todos los procesos en modo -m
    }
}

int main(int argc, char *argv[]) {
    int opt;
    int mode = 0;
    const char *directory = "./archive"; // Carpeta predeterminada

    // Inicializar el mutex
    pthread_mutex_init(&lock, NULL);

    // Manejar las opciones de línea de comandos
    while ((opt = getopt(argc, argv, "smf:")) != -1) {
        switch (opt) {
            case 's':
                mode = 1;
                break;
            case 'm':
                mode = 2;
                break;
            default:
                fprintf(stderr, "Uso: %s [-s | -m] -f carpeta\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    // Verificar el directorio y cargar archivos
    printf("Cargando archivos del directorio: %s\n", directory);
    load_files_from_directory(directory, mode);

    printf("Número total de archivos procesados: %d\n", file_count);

    // Destruir el mutex
    pthread_mutex_destroy(&lock);

    return 0;
}

