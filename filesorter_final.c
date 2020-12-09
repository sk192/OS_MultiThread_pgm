#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<unistd.h>
#include<string.h>
#include<pthread.h>
#include<errno.h>
#include <fcntl.h> 
#include<time.h>

#define true 1
#define false 0

#define READ_AHEAD 3 
#define sleep_time 1

enum thread_types {READER, SORTER, WRITER};

struct thread_info
	{
		enum thread_types my_type;
		pthread_t tid;
		int myid;
		int thread_set;
		int* buf_status; //flags
		int** buf;   //array of buffers.
		int num_files;
		int set_id;
		int rand_num;
		char *directory;
		pthread_mutex_t* lock;
		int num_threads;
		int buf_size;
		char *path;
		
	};	

void *create_files(void *ctin)
{
	
	unsigned int seed;
	struct thread_info* create_tin = (struct thread_info*) ctin;
	int cur_tid = create_tin->myid;
	
	for(int o = 0; o < create_tin->num_files; o++)
	{
		if(o % create_tin->num_threads == cur_tid)
		{
			char s[60000];
			sprintf(s,"%s/unsorted_%d.bin",create_tin->directory,o);
			int fp = open(s,O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
			if(fp == -1)
			{
				printf("\nError! opening/creating file");
			}

			for(int j = 0; j < create_tin->rand_num; j++)
			{
				unsigned x = (rand_r(&seed) % 32767) + 1;
				pthread_mutex_lock(create_tin->lock);
				write(fp,&x,sizeof(x));
				pthread_mutex_unlock(create_tin->lock);
			}
			close(fp);
		}

	}
	
	return NULL;
}

int get_next_file_idx(struct thread_info* tin, int look_for_status, int waiting_status)
{
	
	int done =0;
	int in_progress = 0;
	int workable = 0;
	int work_next = -1;
	for(int k =0 ; k < (tin-> num_files); k++)
	{
		
		int temp = tin->buf_status[k];
		if(temp == 7){
			done++;
		}
		else if(temp <= look_for_status)
		{
			workable++;
			if(temp == look_for_status && work_next == -1){
				work_next = k;
			}
		}
		else{
			in_progress++;
		}
		 
	}
	
	if(workable == 0)
		return -3;
	else if((work_next == -1) || (tin->my_type == READER && in_progress >= READ_AHEAD))
	{
		sleep(sleep_time);
		return -2;
	}
	pthread_mutex_lock(tin->lock);
	int go_ahead = 0;
	if(tin->buf_status[work_next] == look_for_status)
	{
		go_ahead = 1;
		tin->buf_status[work_next] = waiting_status;
	}
	pthread_mutex_unlock(tin->lock);
	if(!go_ahead)
	{
		printf("<tid:%d> Can't go ahead. other reader seems to have grabbed it \n", tin->myid);
		return -2;
	}
	return work_next;
}

void mark_buf_status(int* buf_status, pthread_mutex_t *lock, int idx, int status)
{
	pthread_mutex_lock(lock);
	buf_status[idx] = status;
	pthread_mutex_unlock(lock);
}

void *read_file(void * tin)
{
	struct thread_info *read_tin = (struct thread_info *) tin; 
	while(true)
	{
		
		int next_to_read = get_next_file_idx(read_tin,1,2);
		int file_idx = read_tin->thread_set * next_to_read + read_tin->set_id;

		if(next_to_read == -2)
			continue;
		else if(next_to_read == -3)
			break;
		
		int fp;
		char s[20000];
		sprintf(s,"%s/unsorted_%d.bin",read_tin->directory,file_idx);
		
		fp = open(s,O_RDONLY);
		if(!fp)
		{
			printf("ERROR: opening file\n");
			mark_buf_status(read_tin->buf_status, read_tin->lock, next_to_read, 7);
			continue;
		}
		int num_bytes = read(fp,read_tin->buf[next_to_read],read_tin->rand_num * sizeof(int));
		int count = num_bytes / sizeof(int);
		if(count != read_tin->rand_num)
			mark_buf_status(read_tin->buf_status, read_tin->lock, next_to_read, 7);
		else
			mark_buf_status(read_tin->buf_status, read_tin->lock, next_to_read, 3);
		close(fp);
	}
	return NULL;
}

void swap(int *y1, int *y2)
{
	int temp = *y1;
	*y1 = *y2;
	*y2 = temp;
}

int partition(int *buf, int l,int h)
{
	int pivot = buf[h];
	int o = l - 1;
	for(int k = l; k <= h; k++)
	{
		if(buf[k] < pivot)
		{
			o++;
			swap(&buf[k],&buf[o]);
		}
	}
	swap(&buf[o+1],&buf[h]);
	return (o + 1);
}

void quick_sort(int *buf, int low, int high)
{
	if(low < high)
	{
		int pi = partition(buf, low, high);
		quick_sort(buf, low, pi-1);
		quick_sort(buf, pi+1, high);
	}
}

void *sorting_file(void * tin)
{
	struct thread_info *sort_tin = (struct thread_info *) tin; 
	while(true)
	{
		int next_to_sort = get_next_file_idx(sort_tin, 3,4);
		if(next_to_sort == -2)
			continue;
		else if(next_to_sort == -3)
			break;
		quick_sort(sort_tin->buf[next_to_sort],0,(sort_tin->rand_num)-1);
		mark_buf_status(sort_tin->buf_status,sort_tin->lock,next_to_sort,5);
	}
	return NULL;
}

void *write_file(void * tin)
{
	struct thread_info *write_tin = (struct thread_info *) tin; 
	while(true)
	{
		int next_to_write = get_next_file_idx(write_tin,5,6);
		int file_idx = write_tin->thread_set * next_to_write + write_tin->set_id;
		if(next_to_write == -2)
			continue;
		else if(next_to_write == -3)
			break;
		int fd;
		char ar[10000];

		sprintf(ar,"%s/sorted_%d.bin",write_tin->path,file_idx);
		fd = open(ar,O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
		int fp = write(fd,write_tin->buf[next_to_write],sizeof(int) * write_tin->rand_num);
		if(fp == 0)
			printf("<tid : %d> writing '%s' didn't work\n",write_tin->myid,ar);
		else
			printf("<tid : %d> wrote '%d' numbers in file '%s' \n",write_tin->myid, write_tin->rand_num,ar);
		mark_buf_status(write_tin->buf_status,write_tin->lock,next_to_write,7);
		close(fd);
	}
	return NULL;
}

int main(int argc, char* argv[])
{
	int num_files, T,num;
	printf("\nenter num of files:");
	scanf("%d",&num_files);
	printf("\nenter how many integers you want to write in the file:");
	scanf("%d",&num);
	printf("\nEnter number of threads:");
	scanf("%d",&T);
	int rc;
	char *directory = argv[1];
	printf("%s",directory);
	int t_set = atoi(argv[2]);

	char path[1000]; 
	sprintf(path,"%s/sorted",directory);
	struct stat st1 = {0};
	if (stat(path, &st1) == -1)
	{
	 	printf("creating path\n");
	    mkdir(path, 0700);
	}
	
	struct stat st = {0};
	if (stat(directory, &st) == -1)
	    mkdir(directory, 0700);

	
	struct thread_info **create_child_threads = (struct thread_info**) malloc(T * sizeof(struct thread_info *));
	for(int i = 0; i < T; i++)
	{
		pthread_mutex_t* this_lock = (pthread_mutex_t*) malloc (sizeof(pthread_mutex_t));
		pthread_mutex_init(this_lock, NULL);

		struct thread_info *create_file_tin = (struct thread_info *) malloc (sizeof(struct thread_info)); 

		create_file_tin->myid = i;
		create_file_tin->num_files = num_files;
		create_file_tin->rand_num = num;
		create_file_tin->num_threads = T;
		create_child_threads[i] = create_file_tin;
		create_file_tin->directory = directory;
		create_file_tin->lock = this_lock;


		rc = pthread_create(&create_file_tin->tid, NULL, create_files, (void *) create_file_tin);
		if (rc)
		{
	      printf("ERROR; return code from pthread_create() is %d\n", rc);
	      exit(-1);
    	}
	}
	for(int i = 0; i < T; i++)
	{
		rc = pthread_join(create_child_threads[i]->tid,NULL);
		if (rc != 0) 
		{
	      printf("ERROR: Error in joining thread: %d \n", rc);
	      exit(-1);
    	}
    	free(create_child_threads[i]);
	}
	free(create_child_threads);

	int rc1;
	
	struct thread_info **child_threads = (struct thread_info**) malloc(3 * t_set * sizeof(struct thread_info *));

	for(int j = 0; j < t_set; j++)
	{

		pthread_mutex_t* cur_lock = (pthread_mutex_t*) malloc (sizeof(pthread_mutex_t));

		int file_count = (num_files / t_set) + ((j < (num_files % t_set))?1:0);
		int* cur_buf_status = (int*) malloc(file_count * sizeof(int));
		int **cur_buf = (int**) malloc(file_count * sizeof(int*));
		
		pthread_mutex_init(cur_lock, NULL);
		
		for(int k=0; k < file_count; k++)
		{
			cur_buf[k] = (int *) malloc(num * sizeof(int));
			cur_buf_status[k] = 1;
		}

		struct thread_info *read_file_tin = (struct thread_info *) malloc (sizeof(struct thread_info));   //structure size

		int thread_idx = j * 3;
		read_file_tin->buf_status = cur_buf_status;
		read_file_tin->lock = cur_lock;
		read_file_tin->thread_set = t_set;
		read_file_tin->num_files = file_count;
		read_file_tin->rand_num = num;
		read_file_tin->directory = directory;
		read_file_tin->buf = cur_buf;
		read_file_tin->myid = thread_idx;
		read_file_tin->set_id = j;
		read_file_tin->my_type = READER;
		child_threads[thread_idx] = read_file_tin;
		read_file_tin->path = path;

		struct thread_info *sorting_file_tin = (struct thread_info *) malloc (sizeof(struct thread_info)); 
		thread_idx++;
		sorting_file_tin->buf_status = cur_buf_status;
		sorting_file_tin->lock = cur_lock; 
		sorting_file_tin->my_type = SORTER;
		sorting_file_tin->thread_set = t_set;
		sorting_file_tin->num_files = file_count;
		sorting_file_tin->rand_num = num;
		sorting_file_tin->directory = directory;
		sorting_file_tin->set_id = j;
		sorting_file_tin->buf = cur_buf;
		sorting_file_tin->myid = thread_idx;
		sorting_file_tin->path = path;
		
		child_threads[thread_idx] = sorting_file_tin;

		struct thread_info *write_file_tin = (struct thread_info *) malloc (sizeof(struct thread_info)); 
		thread_idx++;
		write_file_tin->buf_status = cur_buf_status;
		write_file_tin->lock = cur_lock;
		write_file_tin->my_type = WRITER;
		write_file_tin->thread_set = t_set;
		write_file_tin->num_files = file_count;
		write_file_tin->rand_num = num;
		write_file_tin->directory = directory;
		write_file_tin->set_id = j;
		write_file_tin->buf = cur_buf;
		write_file_tin->path = path;
		
		write_file_tin->myid = thread_idx;
		child_threads[thread_idx] = write_file_tin;
		
		rc1 = pthread_create(&read_file_tin->tid,NULL, read_file, (void *) read_file_tin);
		if (rc1)
		{
      		printf("ERROR; return code from pthread_create() is %d\n", rc1);
      		exit(-1);
		}
		
		rc1 = pthread_create(&sorting_file_tin->tid,NULL, sorting_file, (void *) sorting_file_tin);
		if (rc1)
		{
      		printf("ERROR; return code from pthread_create() is %d\n", rc1);
      		exit(-1);
		}

		
		rc1 = pthread_create(&write_file_tin->tid,NULL, write_file, (void *) write_file_tin);
		if (rc1)
		{
      		printf("ERROR; return code from pthread_create() is %d\n", rc1);
      		exit(-1);
		}	
	}
	for(int k =0; k < 3 * t_set; k++)
	{
		rc1 = pthread_join(child_threads[k]->tid,NULL);
		if (rc1 != 0) 
		{
      		printf("ERROR; joining thread: %d\n",rc1);
		}
	}
	return(0);
}