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
		
		if (pos) {
			pos->cur_index = 0;
			pos_record[i] = pos;
		}
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

/*pos >= 0*/
void calc_hash(char *data, unsigned int *window_hash, unsigned int *seg_hash, int pos)
{
	int window_id;

	window_id = (pos/8)/HASH_WINDOW_LEN;

	if (window_id == 0) {
		*window_hash = djb_hash(data + DATA_SIZE - HASH_WINDOW_LEN , HASH_WINDOW_LEN);
		*seg_hash = djb_hash(data, HASH_WINDOW_LEN); 
	} else {
		*window_hash = djb_hash(data, window_id*HASH_WINDOW_LEN);
		*seg_hash = djb_hash_seg (data + window_id*HASH_WINDOW_LEN, *window_hash, HASH_WINDOW_LEN);
	}
}

void calc_hash_by_base(char *data, unsigned int base_hash, int base_pos, unsigned int *window_hash, unsigned int *seg_hash, int pos)
{
	int base_window_id, window_id;

	if(pos <= base_pos)
		spt_assert(0);

	window_id = (pos/8)/HASH_WINDOW_LEN;
	base_window_id = (base_pos/8)/HASH_WINDOW_LEN;

	if (base_window_id == 0) {
		return calc_hash(data, window_hash, seg_hash, pos);
	}

	if (window_id == base_window_id) {
		*window_hash = base_hash;
		*seg_hash = djb_hash_seg(data + base_window_id*HASH_WINDOW_LEN , base_hash, HASH_WINDOW_LEN);
	} else {
		*window_hash = djb_hash_seg(data + base_window_id*HASH_WINDOW_LEN, base_hash, (window_id - base_window_id)*HASH_WINDOW_LEN);
		*seg_hash = djb_hash_seg(data + window_id*HASH_WINDOW_LEN, *window_hash, HASH_WINDOW_LEN);
	}
}
int get_real_pos_start(struct spt_vec *pvec)
{
	struct spt_vec tmp_vec;
	tmp_vec.val = pvec->val;
	if (pvec->scan_status == SPT_VEC_PVALUE) {
		tmp_vec.pos = tmp_vec.pos + 1;
		pos_record[g_thrd_id]->pos_array[0] = tmp_vec.pos;
		pos_record[g_thrd_id]->cur_index = 1;
		return tmp_vec.pos;
	}
	spt_assert(0);
	return -1;
}

int get_real_pos_next(struct spt_vec *pvec)
{
	int real_pos;
	int cur_index;
	struct spt_vec vec;
	vec.val = pvec->val;	
	if (pvec->scan_status == SPT_VEC_PVALUE) {
		cur_index = pos_record[g_thrd_id]->cur_index;
		spt_assert(cur_index >= 1);
		
		vec.pos = vec.pos + 1;
		pos_record[g_thrd_id]->pos_array[cur_index] = vec.pos;
		if(vec.pos <= pos_record[g_thrd_id]->pos_array[cur_index - 1]){
			printf("cur pos :%d, pre pos :%d\r\n", vec.pos, pos_record[g_thrd_id]->pos_array[cur_index-1]);
			printf("cur_index:%d\r\n", cur_index);
			spt_assert(0);
		}
		
		pos_record[g_thrd_id]->cur_index++;
		
		return vec.pos;
	} else if(pvec->scan_status == SPT_VEC_HVALUE) {
		vec.val = pvec->val;
		cur_index = pos_record[g_thrd_id]->cur_index;
		spt_assert(cur_index > 0);
		real_pos = pos_record[g_thrd_id]->pos_array[cur_index - 1];
		real_pos =  (real_pos /32 )*32 + spt_get_pos_offset(vec);
		return real_pos;
	}
	spt_assert(0);
	return -1;
}
void real_pos_back(struct spt_vec *pvec, struct spt_vec *pre_vec)
{
	spt_assert(pos_record[g_thrd_id]->cur_index > 0);

	if(pvec->scan_status == SPT_VEC_PVALUE)
		pos_record[g_thrd_id]->cur_index--;

	if (pre_vec->scan_status == SPT_VEC_PVALUE)
		pos_record[g_thrd_id]->cur_index--;

}

int set_real_pos(struct spt_vec *pvec, unsigned int real_pos, unsigned int pre_pos, unsigned int real_hash)
{
	int cur_window, last_window;
	int offset;

	cur_window = (real_pos/8)/HASH_WINDOW_LEN;
	last_window = (pre_pos/8)/HASH_WINDOW_LEN;

	if (cur_window != last_window) {
		pvec->pos = real_pos - 1;
		pvec->scan_status = SPT_VEC_PVALUE;
		return SPT_VEC_PVALUE;
	} else {
		pvec->scan_status = SPT_VEC_HVALUE;
		offset = real_pos %(8*HASH_WINDOW_LEN);
		pvec->pos = (real_hash << 5) + offset;
		return SPT_VEC_HVALUE;
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
int last_seg_hash_grp;
int last_window_hash;
int find_start_vec(struct cluster_head_t *pclst, struct spt_vec **vec, int *start_pos, char *data, int window)
{
	int gid,fs;
	struct spt_grp *grp, *next_grp, old_grp;
	struct spt_pg_h *spt_pg;
	struct spt_vec cur_vec, *pvec;
	
	unsigned int window_hash, seg_hash, allocmap;
	calc_hash(data, &window_hash, &seg_hash, window);
	//printf("window hash 0x%x, seg hash 0x%x\r\n",window_hash, seg_hash);
	
	gid = seg_hash %GRP_SPILL_START;
	last_window_hash = window_hash;
	last_seg_hash_grp = gid;
	*vec = NULL;
re_find:
	spt_pg = get_vec_pg_head(pclst, gid/GRPS_PER_PG);
	grp = get_grp_from_page_head(spt_pg, gid);
	//printf("gid 0x%x, grp %p\r\n", gid, grp);
	fs = 0;
	
	old_grp.val = grp->val;
	allocmap = old_grp.allocmap;
	allocmap = ~allocmap;
	//printf("allocmap 0x%x\r\n", allocmap);
	while (1) {
		fs = find_next_bit(&allocmap, 32, fs);
		if (fs >= 32) {
			next_grp = grp->next_grp;
			if ((next_grp == 0) || (next_grp == 0xFFFFF))
				return -1;
			gid = next_grp;
			goto re_find;
		}
		pvec = (char *) grp + sizeof(struct spt_grp) + fs *sizeof(struct spt_vec); 
		cur_vec.val = pvec->val;
		//printf("check vec %p\r\n", pvec);	
		if ((cur_vec.scan_status == SPT_VEC_HVALUE) && (cur_vec.status == SPT_VEC_VALID)) {
			if(spt_get_pos_hash(cur_vec) == (window_hash & SPT_HASH_MASK)) {
				*vec = pvec;
				return gid*VEC_PER_GRP + fs;
			}
		}
		fs++;
	}
}
void test_calc_hash(char *data, int pos)
{
	unsigned int window_hash, seg_hash, allocmap;
	calc_hash(data, &window_hash, &seg_hash, pos);
	printf("window hash %d, seg_hash %d\r\n", window_hash, seg_hash);
}
void test_data_grp(unsigned int value)
{
	printf("grp is %d\r\n", value%GRP_SPILL_START);

}
