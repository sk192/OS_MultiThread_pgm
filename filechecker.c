#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<unistd.h>
#include<string.h>
#include<pthread.h>
#include<errno.h>
#include <fcntl.h>
#include<errno.h>
#include<time.h>
#include <sys/mman.h>


struct checker
{
	// int *array;
	// int *array1;
	pthread_t tid;
	int th_id;
	int num_file;
	int num_int;
	int num_threads;
	pthread_mutex_t* lock;
	char *path;
};

void swap(int *y1, int *y2)
{
	int temp = *y1;
	*y1 = *y2;
	*y2 = temp;
}

int partition(int *array, int l,int h)
{
	int pivot = array[h];
	int o = l - 1;
	for(int k = l; k <= h; k++)
	{
		if(array[k] < pivot)
		{
			o++;
			swap(&array[k],&array[o]);
		}
	}
	swap(&array[o+1],&array[h]);
	return (o + 1);
}

void quick_sort(int *array, int low, int high)
{
	if(low < high)
	{
		int pi = partition(array, low, high);
		quick_sort(array, low, pi-1);
		quick_sort(array, pi+1, high);
	}
}

void *file_checker(void *tin)
{
	struct checker *check_tin = (struct checker*) tin;
	for(int j=0; j<check_tin->num_file; j++)
	{
		if((j % check_tin->num_threads)  == check_tin->th_id)
		{
			pthread_mutex_lock(check_tin->lock);
			printf("curr_thread:%d\n",check_tin->th_id);
			char buf[100];
			sprintf(buf,"%s/unsorted_%d.bin",check_tin->path,j);
			printf("reading file:%s\n",buf);

			int fp = open(buf,O_RDWR);
			if(fp == -1)
			{
				printf("\nError opening unsorted file");
			}
			
			int *arr = mmap(NULL, check_tin->num_int * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fp, 0);
			if(arr == MAP_FAILED){
        		printf("arr Mapping Failed\n");
        		exit(-1);
   			 }
			// pthread_mutex_lock(&mutex);
			// int read_count = read(fp,check_tin->array,sizeof(int) * check_tin->num_int);
			// printf("\nTotal number of bytes file contatin:%d\n", read_count);
			for(int t = 0; t < check_tin->num_int; t++)
			{
				printf("before sorting array:%d\n",arr[t]);
			}
			quick_sort(arr,0,(check_tin->num_int) - 1);
			printf("\n");
			for(int t = 0; t < check_tin->num_int; t++)
			{
				printf("after sorting array:%d\n",arr[t]);
			}
			msync(arr, check_tin->num_int * sizeof(int), MS_SYNC);
			int code1 = munmap(arr,check_tin->num_int * sizeof(int));
			if(code1 != 0)
			{
				printf("arr Unmapping failed\n");
				break;
			}
			close(fp);

			char second_buf[100];
			sprintf(second_buf,"%s/sorted/sorted_%d.bin",check_tin->path,j);
			int fp1 = open(second_buf,O_RDWR);
			if(fp1 == -1)
			{
				printf("\nError opening sorted file");
			}
			
			int *arr1 = mmap(NULL, check_tin->num_int * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fp1, 0);
			if(arr1 == MAP_FAILED){
       			 printf("arr1 Mapping Failed\n");
        		break;
    		}
			
			// read(fp1,check_tin->array1,check_tin->num_int * sizeof(int));
			
			for(int k=0 ;k < check_tin->num_int; k++)
			{
				if(arr[k] == arr1[k])
					continue;
				else
				{
					printf("\nfile is not sorted\n");
					exit(-1);
				}
			}
			msync(arr1, check_tin->num_int * sizeof(int), MS_SYNC);
			int code = munmap(arr1,check_tin->num_int * sizeof(int));
			if(code != 0)
			{
				printf("arr1 Unmapping failed\n");
				break;
			}
			
			pthread_mutex_unlock(check_tin->lock);
			close(fp1);
		}
		
		
	}
	return NULL;
}


int main(int argc, char *argv[])
{
	char *path = argv[1];
	int num_threads = atoi(argv[2]);
	int num_file, num_int;
	printf("num_threads:%d\n", num_threads);
	printf("Enter number of files you want to check. \n");
	scanf("%d",&num_file);
	printf("Enter number of integers that file contain. \n");
	scanf("%d",&num_int);

	struct stat sb;
	if (stat(path, &sb) == -1) 
	{
        printf("Error: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }
   
    //int *arr = (int *) malloc(num_int * sizeof(int));
    //int *arr1 = (int *) malloc(num_int * sizeof(int));

    struct checker **create_child_threads = (struct checker**) malloc(sizeof(struct checker*));
    for(int i = 0; i < num_threads; i++)
    {
    	pthread_mutex_t* this_lock = (pthread_mutex_t*) malloc (sizeof(pthread_mutex_t));
		pthread_mutex_init(this_lock, NULL);
    	struct checker *checker_tin = (struct checker *) malloc (sizeof(struct checker));

    	checker_tin->th_id = i;
    	checker_tin->num_file = num_file;
    	checker_tin->num_int = num_int;
    	// checker_tin->array = arr;
    	// checker_tin->array1 = arr1;
    	create_child_threads[i] = checker_tin;
    	checker_tin->num_threads = num_threads;
    	checker_tin->path = path;
    	checker_tin->lock = this_lock;
    	
    	int rc = pthread_create(&checker_tin->tid, NULL,file_checker,(void *) checker_tin);

    	if(rc)
    	{
    		printf("ERROR: return code from creating thread is:%d\n",rc);
    		exit(-1);
    	}

    }
    for(int j=0; j< num_threads; j++)
    {
    	int rc = pthread_join(create_child_threads[j]->tid,NULL);
    	if(rc)
    	{
    		printf("ERROR: error in joining thread:%d\n", rc);
    	}
    }
    printf("\nAll file sorted correctly. Exiting with success\n");
    return(0);
}
