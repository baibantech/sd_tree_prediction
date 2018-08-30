/*************************************************************************
	> File Name: hash_strategy.c
	> Author: lijiyong0303
	> Mail: lijiyong0303@163.com 
	> Created Time: Thu 02 Aug 2018 05:45:34 PM CST
 ************************************************************************/

#include<stdio.h>
#include "chunk.h"
#include "hash_strategy.h"
struct hash_calc_proc *phash_calc[THREAD_NUM_MAX]; 
struct precise_pos_record  *pos_record[THREAD_NUM_MAX];

int hash_calc_process_init(int thread_num)
{
	int i;
	struct hash_calc_proc *ptr;
	struct precise_pos_record *pos;
	if (thread_num > THREAD_NUM_MAX)
		return -1;

	for (i =0; i < thread_num; i++) {
		ptr = (struct hash_calc_proc *)spt_alloc_zero_page();
		if (ptr) {
			ptr->max_item = (PG_SIZE - sizeof(struct hash_calc_proc)) /sizeof (struct hash_calc_result);
			phash_calc[i] = ptr;
		}
		else
			return -1;

		pos = (struct precise_pos_record *)spt_alloc_zero_page();
		
		if (pos)
				pos->cur_index = 0;
		else
			return -1;
	}
	return 0;
}
struct hash_calc_result *alloc_hash_result(struct hash_calc_proc *thread)
{
	int index;
	struct hash_calc_result *result, *tmp;

	index = thread->alloc_id++;
	if (index <= thread->max_item) {
		index--;
		return &(thread->hresult[index]);
	} else {
		result = spt_malloc(sizeof(struct hash_calc_result));
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

void calc_hash_by_base(char *data, unsigned int base_hash, int base_pos, unsigned int *window_hash, unsigned int *seg_hash, int pos)
{
	int base_window_id, window_id;

	if(pos <= base_pos)
		spt_assert(0);
	window_id = pos > -1 ? (pos/8)/HASH_WINDOW_LEN : 0 ;
	base_window_id = base_pos > -1 ? (base_pos/8)/HASH_WINDOW_LEN : 0 ;

	if (window_id == base_window_id) {
		*window_hash = base_hash;
		if (base_window_id == -1)
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
int get_real_pos_start(struct spt_vec *pvec)
{
	if (pvec->scan_status == SPT_VEC_PVALUE) {
		pos_record[g_thrd_id]->pos_array[0] = pvec->pos;
		pos_record[g_thrd_id]->cur_index = 1;
		return pvec->pos;
	}
	spt_assert(0);
	return -1;
}

int get_real_pos_next(struct spt_vec *pvec)
{
	int real_pos;
	int cur_index;
	struct spt_vec vec;
	
	if (pvec->scan_status == SPT_VEC_PVALUE) {
		cur_index = pos_record[g_thrd_id]->cur_index++;
		pos_record[g_thrd_id]->pos_array[cur_index] = pvec->pos;
		if (cur_index)
			spt_assert(pvec->pos > pos_record[g_thrd_id]->pos_array[cur_index - 1]);
		return pvec->pos;
	} else if(pvec->scan_status == SPT_VEC_HVALUE) {
		vec.val = pvec->val;
		cur_index = pos_record[g_thrd_id]->cur_index;
		real_pos = pos_record[g_thrd_id]->pos_array[cur_index];
		real_pos =  real_pos + spt_get_pos_offset(vec);
		return real_pos;
	}
	spt_assert(0);
	return -1;
}
void real_pos_back(struct spt_vec *pvec, struct spt_vec *pre_vec)
{
	spt_assert(pos_record[g_thrd_id]->cur_index > 0);

	if(pvec->scan_status == SPT_VEC_HVALUE) {
		if (pre_vec->scan_status == SPT_VEC_HVALUE)
			return;
		pos_record[g_thrd_id]->cur_index--;
	} else {
		pos_record[g_thrd_id]->cur_index--;
	}	
}

void set_real_pos(struct spt_vec *pvec, unsigned int real_pos, unsigned int pre_pos, unsigned int real_hash)
{
	int cur_window, last_window;
	int offset;

	cur_window = (real_pos/8)/HASH_WINDOW_LEN;
	last_window = (pre_pos/8)/HASH_WINDOW_LEN;

	if (cur_window != last_window) {
		pvec->pos = real_pos - 1;
		pvec->scan_status = SPT_VEC_PVALUE;
	} else {
		pvec->scan_status = SPT_VEC_HVALUE;
		offset = real_pos %(8*HASH_WINDOW_LEN);
		pvec->pos = (real_hash << 5) + offset;
	}
}
int is_need_chg_pos(struct spt_vec *vec, struct spt_vec *next_vec, int type)
{
	if (type == SPT_OP_INSERT) {
			if ((vec->scan_status == SPT_VEC_PVALUE)&&
					(next_vec->scan_status == SPT_VEC_PVALUE))
				return 1;
	} else if (type == SPT_OP_DELETE){
			if ((vec->scan_status == SPT_VEC_PVALUE)&&
					(next_vec->scan_status == SPT_VEC_HVALUE))
				return 1;
	}
	return 0;
}
