/*
 * Imports
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <mpi.h>
#include <omp.h>
#define SIZE 10000
/* Constants */

#define PRINT_VECS 1  // flag so we can turn off printing when N is large
#define MAX_RAND 100  // max value of elements generated for array

/* Prototypes */
void init_vec(int *vec, int len);
void print_vec(const char *label, int *vec, int len);

/*creating queue structure */

struct Queue {
    int items[SIZE];
    int front;
    int rear;
};
/* queue Functions */
struct Queue* createQueue() {
    struct Queue* q = malloc(sizeof(struct Queue));
    q->front = -1;
    q->rear = -1;
    return q;
}


int isEmpty(struct Queue* q) {
    if (q->rear == -1)
        return 1;
    return 0;
}

void enqueue(struct Queue* q, int value) {
    if (q->rear == SIZE - 1)
        return;
    else {
        if (q->front == -1)
            q->front = 0;
        q->rear++;
        q->items[q->rear] = value;
    }
}

int dequeue(struct Queue* q) {
    int item;
    if (isEmpty(q)) {
        item = -1;
    }
    else {
        item = q->items[q->front];
        q->front++;
        if (q->front > q->rear) {
            q->front = q->rear = -1;
        }
    }
    return item;
}

void printQueue(struct Queue *q) {
    int i = q->front;

    if (isEmpty(q)) {
    } else {
        for (i = q->front; i < q->rear + 1; i++) {
            printf("%d ", q->items[i]);
        }
    }
    printf("\n");
}


int isVInQueue(struct Queue *q, int v) {
    int i = q->front;

    if (isEmpty(q)) {
    } else {
        for (i = q->front; i < q->rear + 1; i++) {
            if (v == q->items[i])
                return 1;
        }
    }
    return 0;
}


void assignLocalAndRemoteVertices(struct Queue *local, struct Queue *remote, struct Queue *q, int rank, int owner[]) {
    int i = q->front;

    if (isEmpty(q)) {
    } else {
        for (i = q->front; i < q->rear + 1; i++) {
            int v = q->items[i];
            if (owner[v] == rank)
                enqueue(local, v);
            else enqueue(remote, v);
        }
    }
}

/* file loading functions */
void load(int **graph, char* file_name)
{
    FILE *load;
    load = fopen(file_name, "r");
    if (load == NULL)
        printf("\nFile cannot be opened.\n");
    else {
        // get name of graph
        char name[100];
        fgets(name, 100, load);
        //printf("Name: %s\n", name);
        char vertices[10];
        fgets(vertices, 10, load);
        // While program doesn't reach end of file
        while (!feof(load)) {
            // Taking input from file in new record
            // Have to make the \n character null because fgets gets \n from file whereas gets ignores \n when taking input from user
            char input_line[1000];
            fgets(input_line, 1000, load);
            char *edge;
            /* get the first token */
            edge = strtok(input_line, " ");
            int u = atoi(edge);
            edge = strtok(NULL, " ");
            /* walk through other tokens */
            while ( edge != NULL ) {
                int v = atoi(edge);
                graph[u - 1][v - 1] = 1;
                graph[v - 1][u - 1] = 1;
                edge = strtok(NULL, " ");
            }
        }
    }
    fclose(load);
}

//Get number of vertices of Graph
int get_n(char* file_name)
{
    int n = 0;
    FILE *load;
    load = fopen(file_name, "r");
    if (load == NULL)
        printf("\nFile cannot be opened.\n");
    else {
        // get name of graph
        char name[100];
        fgets(name, 100, load);
        char vertices[10];
        fgets(vertices, 10, load);
        // get n = number of vertices of graph
        n = atoi(vertices);
    }
    fclose(load);
    return n;
}

/*other functions*/
// Fills a vector with random integers in the range [0, MAX_RAND)
void init_vec(int *vec, int len)
{
    int i;
    for (i = 0; i < len; i++)
    {
        vec[i] = rand() % MAX_RAND;
    }
}

// Prints the given vector to stdout
void print_vec(const char *label, int *vec, int len)
{
#if PRINT_VECS
    printf("%s", label);

    int i;
    for (i = 0; i < len; i++)
    {
        printf("%d ", vec[i]);
    }
    printf("\n\n");
#endif
}


void bfs_sequential(int** graph, int source, int n)
{

    int visited[n];
    struct Queue* q = createQueue();
    for (int i = 0; i < n; i++)
        visited[i] = 0;

    visited[source] = 1;
    enqueue(q, source);

    while (!isEmpty(q)) {
        int u = dequeue(q);

        for (int v = 0; v < n; v++)
        {
            if (graph[u][v] == 1)
            {
                if (visited[v] == 0) {
                    visited[v] = 1;
                    enqueue(q, v);
                }
            }
        }
    }
}


void bfs_sequential_top_down(int** graph, int source, int n)
{
    int parent[n];
    for (int i = 0; i < n; i++)
        parent[i] = -1;
    parent[source] = source;

    struct Queue* frontier = createQueue();
    enqueue(frontier, source);

    struct Queue* next = NULL;

    while (frontier != NULL)
    {
        while (!isEmpty(frontier)) {
            int u = dequeue(frontier);

            for (int v = 0; v < n; v++)
            {
                if (graph[u][v] == 1)
                {
                    if (next == NULL)
                    {
                        next = createQueue();
                    }
                    if (parent[v] == -1)
                    {
                        enqueue(next, v);
                        parent[v] = u;
                    }
                }
            }
        }
        frontier = next; next = NULL;
    }
}




void bfs_sequential_bottom_up(int** graph, int source, int n)
{
    int parent[n];
    for (int i = 0; i < n; i++)
        parent[i] = -1;
    parent[source] = source;

    struct Queue* frontier = createQueue();
    enqueue(frontier, source);

    struct Queue* next = NULL;

    while (frontier != NULL)
    {
        for (int u = 0; u < n; u++)
        {
            if (parent[u] == -1)
            {
                for (int v = 0; v < n; v++)
                {
                    if (graph[u][v] == 1 && isVInQueue(frontier, v) == 1)
                    {
                        if (next == NULL)
                        {
                            next = createQueue();
                        }
                        enqueue(next, u);
                        parent[u] = v;
                        break;
                    }
                }
            }

        }
        frontier = next; next = NULL;
    }
}

int main(int argc, char *argv[])
{
    // Declare process-related vars
    // and initialize MPI
    int rank;
    int num_procs;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); //grab this process's rank
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs); //grab the total num of processes
    printf("rank: %d\n", rank);
    int** graph;

    double start_time;
    double stop_time;

    char* file_name = "Sparse500.txt";
    int n = get_n(file_name);
    // init graph nxn matrix
    graph = (int **)malloc(n * sizeof(int *));
    for (int i = 0; i < n; i++)
    {
        graph[i] = (int *)malloc(n * sizeof(int));
        for (int j = 0; j < n; j++)
        {
            graph[i][j] = 0;
        }

    }


    load(graph, file_name);

    if (!rank)
    {
        int edges = 0;
        for (int i = 0; i < n; i++)
        {
            for (int j = 0; j < n; j++)
            {
                if (graph[i][j] == 1)
                    edges++;
            }

        }
        printf("Edges: %d\n", edges);
        printf("Procs: %d\n", num_procs);
        printf("File: %s\n", file_name);
    }
    srand(time(NULL));
    start_time = MPI_Wtime();
    printf("rank: %d\n", rank);

    int chunk_size = n / num_procs;
    int owner[n];
    // assign first chunk to proc 0
    for (int i = 0; i < chunk_size; i++)
    {
        owner[i] = 0;
    }
    // assign othe chunks to respective processors
    int curr_owner = 0;
    for (int i = chunk_size; i < n; i++)
    {
        if (i % chunk_size == 0 && curr_owner < num_procs - 1)
            curr_owner++;
        owner[i] = curr_owner;
    }

    int source = 0;
    int visited[n];
    // init visited array for each proc
    for (int i = 0; i < n; i++)
        visited[i] = 0;
    // init queue for each proc
    struct Queue* frontier = createQueue();
    // if proc 0, enqueue root
    if (rank == 0)
    {
        for (int v = 0; v < n; v++)
        {
            if (graph[source][v] == 1)
            {
                enqueue(frontier, v);
            }
        }
        visited[source] = 1;
    }

    // init 2D send buffer for each proc
    int **sendBuf = (int **)malloc(num_procs * sizeof(int *));
    for (int i = 0; i < num_procs; i++)
    {
        sendBuf[i] = (int *)malloc(n * sizeof(int));
        for (int j = 0; j < n; j++)
            sendBuf[i][j] = 0;
    }

    // init 1D recv buffer for each proc
    int* recvBuf = (int *)malloc(n * sizeof(int));
    int sendTo[n];
    for (int i = 0; i < n; i++)
        sendTo[i] = 0;

    // BFS
    while (frontier != NULL)
    {
        struct Queue* remote = createQueue();
        struct Queue* local = createQueue();
        // set local and remote vertices
        assignLocalAndRemoteVertices(local, remote, frontier, rank, owner);
        free(frontier);
        frontier = NULL;
        // add vertices from remote to send buf
        while (!isEmpty(remote))
        {
            int v = dequeue(remote);
            sendBuf[owner[v]][v] = 1;
            sendTo[owner[v]] = 1;
        }

        // send respective remote vertices
        MPI_Request request;
        for (int proc = 0; proc < num_procs; proc++)
        {
            if (proc != rank && sendTo[proc] == 1)
            {
                MPI_Isend(sendBuf[proc], n, MPI_INT, proc, 1, MPI_COMM_WORLD, &request);
                sendTo[proc] = 0;
            }
        }

        // process local vertices
        while (!isEmpty(local))
        {
            int v = dequeue(local);
            if (visited[v] == 0)
            {
                int u;
                #pragma omp parallel for private(u)
                for (u = 0; u < n; u++)
                    if (graph[v][u] == 1)
                    {
                        #pragma omp critical
                        {
                            if (frontier == NULL)
                            {
                                frontier = createQueue();
                            }
                            enqueue(frontier, u);
                        }
                    }
                visited[v] = 1;
            }
        }

        // recv vertices from remote and enqueue neighbours in frontier
        MPI_Status status;
        MPI_Recv(recvBuf, n, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        int j;
        #pragma omp parallel for private(j)
        for (j = 0; j < n; j++)
        {
            if (recvBuf[j] == 1)
            {
                if (visited[j] == 0)
                {
                    for (int k = 0; k < n; k++)
                    {
                        if (graph[j][k] == 1)
                        {
                            #pragma omp critical
                            {
                                if (frontier == NULL)
                                {
                                    frontier = createQueue();
                                }
                                enqueue(frontier, k);
                            }
                        }
                    }

                    visited[j] = 1;
                }

            }
        }
    }


    MPI_Barrier(MPI_COMM_WORLD); //to wait for every process

    if (!rank)
    {
        stop_time = MPI_Wtime();
        printf("Total time (sec): %f\n", stop_time - start_time);
        start_time = MPI_Wtime();
        bfs_sequential(graph, 0, n);
        stop_time = MPI_Wtime();
        printf("Total time BFS_Sequential (sec): %f\n", stop_time - start_time);
        printf("BFS TOP DOWN\n");
        start_time = MPI_Wtime();
        bfs_sequential_top_down(graph, 0, n);
        stop_time = MPI_Wtime();
        printf("Total time BFS_Sequential Top-Down (sec): %f\n", stop_time - start_time);
        printf("BFS BOTTOM UP\n");
        start_time = MPI_Wtime();
        bfs_sequential_bottom_up(graph, 0, n);
        stop_time = MPI_Wtime();
        printf("Total time BFS_Sequential Bottom-Up (sec): %f\n", stop_time - start_time);
    }


    MPI_Finalize();

    return EXIT_SUCCESS;;
}
//clang -Xpreprocessor -fopenmp breadhfirsthybrid.c -n 4 -lomp -lmpi  -o test
