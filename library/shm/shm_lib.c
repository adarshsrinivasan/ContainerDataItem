#include <stdio.h>
#include <stdlib.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <string.h>

struct ipc_perm_new {
    uid_t           uid;            /* Owner's user ID */
    gid_t           gid;            /* Owner's group ID */
    uid_t           cuid;           /* Creator's user ID */
    gid_t           cgid;           /* Creator's group ID */
    mode_t          mode;           /* Read/write permission */
};

struct shmid_ds_new {
    struct ipc_perm_new shm_perm; /* Operation permission value */
    size_t          shm_segsz;      /* Size of segment in bytes */
    pid_t           shm_lpid;       /* PID of last shared memory op */
    pid_t           shm_cpid;       /* PID of creator */
    shmatt_t        shm_nattch;     /* Number of current attaches */
    time_t          shm_atime;      /* Time of last shmat() */
    time_t          shm_dtime;      /* Time of last shmdt() */
    time_t          shm_ctime;      /* Time of last shmctl() change */
};


// Function prototypes
int Create(key_t key, size_t size, int flags);
void* Attach(int shmid);
int Detach(void* ptr);
struct shmid_ds_new* Stat(int shmid);
int Set(int shmid, uid_t uid, gid_t gid, mode_t mode);
int Remove(int shmid);
void Write(void* ptr, const char* str);
char* Read(void* ptr);
void Clear(void* ptr, size_t size);

// Function implementations
int Create(key_t key, size_t size, int flags) {
    int shmid = shmget(key, size, flags);
    if (shmid < 0) {
        perror("Create: shmget");
    }
    return shmid;
}

void* Attach(int shmid) {
    void* ptr = shmat(shmid, NULL, 0);
    if (ptr == (void*)-1) {
        perror("Attach: shmat");
        return NULL;
    }
    return ptr;
}

int Detach(void* ptr) {
    int ret = shmdt(ptr);
    if (ret < 0) {
        perror("Detach: shmdt");
    }
    return ret;
}

struct shmid_ds_new* Stat(int shmid) {
    struct shmid_ds* buf = (struct shmid_ds*)calloc(1, sizeof(struct shmid_ds));
    int ret = shmctl(shmid, IPC_STAT, buf);
    if (ret < 0) {
        perror("Stat: shmctl");
        return NULL;
    }
    struct ipc_perm_new* ipc_perm_new = (struct ipc_perm_new*)calloc(1, sizeof(struct ipc_perm_new));
    struct shmid_ds_new* shmid_ds_new = (struct shmid_ds_new*)calloc(1, sizeof(struct shmid_ds_new));
    ipc_perm_new->uid = buf->shm_perm.uid;
    ipc_perm_new->gid = buf->shm_perm.gid;
    ipc_perm_new->cuid = buf->shm_perm.cuid;
    ipc_perm_new->cgid = buf->shm_perm.cgid;
    ipc_perm_new->mode = buf->shm_perm.mode;

    shmid_ds_new->shm_perm = *ipc_perm_new;
    shmid_ds_new->shm_segsz = buf->shm_segsz;
    shmid_ds_new->shm_lpid = buf->shm_lpid;
    shmid_ds_new->shm_cpid = buf->shm_cpid;
    shmid_ds_new->shm_nattch = buf->shm_nattch;
    shmid_ds_new->shm_atime = buf->shm_atime;
    shmid_ds_new->shm_dtime = buf->shm_dtime;
    shmid_ds_new->shm_ctime = buf->shm_ctime;

    return shmid_ds_new;
}

int Set(int shmid, uid_t uid, gid_t gid, mode_t mode) {
    struct shmid_ds* buf = (struct shmid_ds*)calloc(1, sizeof(struct shmid_ds));
    buf->shm_perm.uid = uid;
    buf->shm_perm.gid = gid;
    buf->shm_perm.mode = mode;
    int ret = shmctl(shmid, IPC_SET, buf);
    if (ret < 0) {
        perror("Set: shmctl");
    }
    return ret;
}

int Remove(int shmid) {
    int ret = shmctl(shmid, IPC_RMID, NULL);
    if (ret < 0) {
        perror("Remove: shmctl");
    }
    return ret;
}

void Write(void* ptr, const char* str) {
    strcpy((char*)ptr, str); // Copy the string to the shared memory
}

char* Read(void* ptr) {
    return (char*)ptr; // Return the string from the shared memory
}

void Clear(void* ptr, size_t size) {
    memset(ptr, 0, size);
}

void Test() {
    key_t key = 5678;
    size_t size = 1024;

//    int shmid = Create(key, size, IPC_CREAT | 0644); // Create shared memory
    int shmid = 1900545;
    void *ptr = Attach(shmid);  // Attach to the shared memory

    struct shmid_ds_new *shmds = Stat(shmid);
    // Print the shmid_ds information
    printf("\nshmid_ds Information:\n");
    printf("key: %d\n", key);
    printf("shmid: %d\n", shmid);
    printf("Mode: %o\n", shmds->shm_perm.mode);
    printf("UID: %d\n", shmds->shm_perm.uid);
    printf("GID: %d\n", shmds->shm_perm.gid);
    printf("Size: %lu bytes\n", (unsigned long) shmds->shm_segsz);
    printf("Last attach time: %ld\n", shmds->shm_atime);
    printf("Last detach time: %ld\n", shmds->shm_dtime);
    printf("Last change time: %ld\n", shmds->shm_ctime);
    printf("Number of attaches: %lu\n", (unsigned long) shmds->shm_nattch);

//    Set(shmid, 0, 0, 0600);
//
//    struct shmid_ds_new *shmds_new1 = Stat(shmid);
//    // Print the shmid_ds information
//    printf("\nshmid_ds Information:\n");
//    printf("key: %d\n", key);
//    printf("shmid: %d\n", shmid);
//    printf("Mode: %o\n", shmds_new1->shm_perm.mode);
//    printf("UID: %d\n", shmds_new1->shm_perm.uid);
//    printf("GID: %d\n", shmds_new1->shm_perm.gid);
//    printf("Size: %lu bytes\n", (unsigned long) shmds_new1->shm_segsz);
//    printf("Last attach time: %ld\n", shmds_new1->shm_atime);
//    printf("Last detach time: %ld\n", shmds_new1->shm_dtime);
//    printf("Last change time: %ld\n", shmds_new1->shm_ctime);
//    printf("Number of attaches: %lu\n", (unsigned long) shmds_new1->shm_nattch);
//
//    Set(shmid, shmds->shm_perm.uid, shmds->shm_perm.gid, shmds->shm_perm.mode);
//
//    struct shmid_ds_new *shmds_new2 = Stat(shmid);
//    // Print the shmid_ds information
//    printf("\nshmid_ds Information:\n");
//    printf("key: %d\n", key);
//    printf("shmid: %d\n", shmid);
//    printf("Mode: %o\n", shmds_new2->shm_perm.mode);
//    printf("UID: %d\n", shmds_new2->shm_perm.uid);
//    printf("GID: %d\n", shmds_new2->shm_perm.gid);
//    printf("Size: %lu bytes\n", (unsigned long) shmds_new2->shm_segsz);
//    printf("Last attach time: %ld\n", shmds_new2->shm_atime);
//    printf("Last detach time: %ld\n", shmds_new2->shm_dtime);
//    printf("Last change time: %ld\n", shmds_new2->shm_ctime);
//    printf("Number of attaches: %lu\n", (unsigned long) shmds_new2->shm_nattch);
//
//    Write(ptr, "testdata1");
    printf("\nData: %s\n", Read(ptr));
    Clear(ptr, size);
    printf("\nData after clear: %s\n", Read(ptr));

    Detach(ptr);              // Detach from the shared memory
    Remove(shmid);              // Remove the shared memory segment
}

int main() {
    Test();
    return 0;
}