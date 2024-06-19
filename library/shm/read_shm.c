#include <stdio.h>
#include <stdlib.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <unistd.h>

void read_shared_memory(key_t key, size_t size) {
    // Get the shared memory ID
    int shmid = shmget(key, size, 0666);
    if (shmid == -1) {
        perror("shmget");
        exit(EXIT_FAILURE);
    }

    // Attach the shared memory segment to the process's address space
    void *ptr = shmat(shmid, NULL, SHM_RDONLY);
    if (ptr == (void *) -1) {
        perror("shmat");
        exit(EXIT_FAILURE);
    }

    // Print the contents of the shared memory
    write(STDOUT_FILENO, ptr, size);

    // Detach the shared memory segment
    if (shmdt(ptr) == -1) {
        perror("shmdt");
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <shm_key> <size>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    key_t shm_key = (key_t) strtoul(argv[1], NULL, 0);
    size_t size = (size_t) strtoul(argv[2], NULL, 0);

    read_shared_memory(shm_key, size);

    return 0;
}

// gcc -o read_shm read_shm.c
// ./read_shm <key> <size>
