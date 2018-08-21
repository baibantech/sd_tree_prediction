/*************************************************************************
	> File Name: hash_strategy.c
	> Author: lijiyong0303
	> Mail: lijiyong0303@163.com 
	> Created Time: Thu 02 Aug 2018 05:45:34 PM CST
 ************************************************************************/

#include<stdio.h>
#include "hash_strategy.h"
struct hash_calc_proc* phash_calc[HASH_CALC_THREAD_NUM]; 
int last_pure_pos[HASH_CALC_THREAD_NUM];
int last_hash_pos[HASH_CALC_THREAD_NUM];
int hash_calc_process_init(int thread_num)
{
	int i;
	struct hash_calc_proc *ptr;
	if (thread_num > HASH_CALC_THREAD_NUM)
		return -1;

	for (i =0; i < thread_num; i++) {
		last_pure_pos[i] = -1;
		last_hash_pos[i] = -1;
		ptr = (struct hash_calc_proc *)spt_alloc_zero_page();
		if (!ptr) {
			ptr->max_item = (PG_SIZE - sizeof(struct hash_calc_proc)) /sizeof (struct hash_calc_result);
			phash_calc[i] = ptr;
		}
		else
			return -1;
	}
	return 0;
}
struct hash_calc_result *alloc_hash_result(struct hash_calc_proc *thread)
{
	int index;
	struct hash_calc_result *resluti, *tmp;
	index = thread->alloc_id++;
	if (index <= thread->max_item) {
		index--;
		return &(thread->hresult[index]);
	} else {
		reslut = spt_malloc(sizeof(struct hash_calc_result));
		if (result) {
			result->window_id = -1;
			result->result = -1;
			tmp = thread->next;
			thread->next = result;
			result->next = tmp;
			return result;
		}
	}
	return NULL;		
}
unsigned int djb_hash(char *data, int len)
{
	unsigned int hash = 5381;
	
	while(len--)
		hash = ((hash << 5) + hash) + (*data++);
	
	return hash;
}

unsigned int djb_hash_seg(char *data, unsigned int src_hash, int len)
{
	unsigned int hash = src_hash;
	
	while(len--)
		hash = ((hash << 5) + hash) + (*data++);
	
	return hash;
}

void calc_hash(char *data, unsigned int *window_hash, unsigned int *seg_hash, int pos)
{
	unsigned int hash ;
	int window_id;
	if (pos == -1) {
		window_id = 0;
	} else
		window_id = (pos/8)/HASH_WINDOW_LEN;

	window_id--;
	if (window_id == -1) {
		*window_hash = 0;
		*seg_hash = djb_hash(data, HASH_WINDOW_LEN); 
	} else {
		*window_hash = djb_hash(data, (window_id+1)*HASH_WINDOW_LEN);
		*seg_hash = djb_hash_seg (data + (window_id + 1)*HASH_WINDOW_LEN, *window_hash, HASH_WINDOW_LEN);
	}
}

void calc_hash_by_base(char *data, unsigned int base_hash, int base_pos, unsigned int *window_hash, unsigned int seg_hash, int pos)
{
	
	unsigned int hash ;
	int base_window_id, window_id;
	if(pos <= base_pos)
		spt_assert(0);
	window_id = pos > -1 ? (pos/8)/HASH_WINDOW_LEN : 0 ;
	base_window_id = base_pos > -1 ? (base_pos/8)/HASH_WINDOW_LEN : 0 ;

	if (window_id == base_window_id) {
		*window_hash = base_hash;
		if (base_window_id = -1)
			*seg_hash = djb_hash(data, HASH_WINDOW_LEN);
		else
			*seg_hash = djb_hash_seg(data + (base_window_id + 1)*HASH_WINDOW_LEN , base_hash, HASH_WINDOW_LEN);
	} else {
		if(base_window_id == -1) {
			*window_hash = djb_hash(data, (window_id + 1)*HASH_WINDOW_LEN);
			*seg_hash = djb_hash_seg(data + (window_id +1)*HASH_WINDOW_LEN, *window_hash, HASH_WINDOW_LEN);
		} else {
			*window_hash = djb_hash_seg(data + (base_window_id + 1)*HASH_WINDOW_LEN, base_hash, (window_id - base_window_id)*HASH_WINDOW_LEN);
			*seg_hash = djb_hash_seg(data + (window_id +1)*HASH_WINDOW_LEN, *window_hash, HASH_WINDOW_LEN);
		}
	}
}

