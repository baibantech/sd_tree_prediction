/*************************************************************************
	> File Name: hash_strategy.c
	> Author: lijiyong0303
	> Mail: lijiyong0303@163.com 
	> Created Time: Thu 02 Aug 2018 05:45:34 PM CST
 ************************************************************************/

#include<stdio.h>
#include "chunk.h"
#include "hash_strategy.h"

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
void calc_grama_hash(char *data, unsigned int *window_hash, unsigned int *seg_hash, int pos)
{
	int cur_pos_len;
	int seg_len;
	int window_len;
	char *cur_byte;
	int window_num;

	cur_byte = data + (pos/8);
	cur_pos_len = window_len = seg_len = pos / 8;

	/*get gramma seg boundary*/
	if (*cur_byte == gramma_window_symbol) {
		cur_byte++;
		seg_len++;
	}
	
	while (*cur_byte != gramma_window_symbol){
		seg_len++; 
		if (seg_len == DATA_SIZE)
			break;
		cur_byte++;
	}
	
	/*get window boundary*/
	cur_byte = data + (pos / 8);
	while(*cur_byte != gramma_window_symbol) {
		window_len--;
		cur_byte--;
	}
	window_num = (cur_pos_len - window_len) / HASH_WINDOW_LEN + 1; 
	spt_trace("windown len %d, seg len %d, window num %d\r\n", window_len ,seg_len ,window_num);	
	if (seg_len - window_len > window_num * HASH_WINDOW_LEN) {
		*window_hash = djb_hash(data, window_len + HASH_WINDOW_LEN);
		*window_hash = *window_hash + window_num - 1;
		*seg_hash = djb_hash_seg(data + window_len + HASH_WINDOW_LEN, *window_hash, seg_len - window_len - HASH_WINDOW_LEN); 
	} else 
		*window_hash = *seg_hash = djb_hash(data, seg_len);

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
int roll_pos_back(struct spt_vec cur_vec)
{
	if (cur_vec.scan_status == SPT_VEC_PVALUE )
		return 1;
	return 0;
}
int set_real_pos(struct spt_vec *pvec, unsigned int real_pos, unsigned int pre_pos, unsigned int real_hash)
{
	int cur_window, last_window;
	int offset;
	if (real_pos <= pre_pos)
		spt_assert(0);
	if (pre_pos > 2048)
		spt_assert(0);

	cur_window =  real_pos/HASH_WINDOW_BIT_NUM;
	last_window = pre_pos/HASH_WINDOW_BIT_NUM;

	if (cur_window != last_window) {
		pvec->pos = real_pos - 1;
		pvec->scan_status = SPT_VEC_PVALUE;
		return SPT_VEC_PVALUE;
	} else {
		pvec->scan_status = SPT_VEC_HVALUE;
		offset = real_pos % HASH_WINDOW_BIT_NUM;
		pvec->pos = (real_hash << SPT_POS_BIT) + offset;
		return SPT_VEC_HVALUE;
	}
}

int is_need_chg_pos(struct spt_vec *vec, struct spt_vec *next_vec, int type)
{
	if (type == SPT_OP_INSERT) {
			if ((vec->scan_status == SPT_VEC_PVALUE)&&
					(next_vec->scan_status == SPT_VEC_PVALUE)) {
				if ((next_vec->pos / HASH_WINDOW_BIT_NUM) == (vec->pos / HASH_WINDOW_BIT_NUM))
					return 1;
			}
	} else if (type == SPT_OP_DELETE){
			if ((vec->scan_status == SPT_VEC_PVALUE)&&
					(next_vec->scan_status == SPT_VEC_HVALUE))
				return 1;
	}
	return 0;
}

void add_real_pos_record(struct cluster_head_t *pclst, struct spt_vec *pvec,int real_pos)
{
	char *pos_mem = pclst->cluster_pos_mem;
	char * record_ptr = pos_mem + ((unsigned long long )(long)(void*)pvec - (unsigned long long)(long)(void*)pclst->cluster_vec_mem);
	struct spt_vec_debug_info *debug = record_ptr;
	debug->pos = real_pos;
}

int get_real_pos_record(struct cluster_head_t *pclst, struct spt_vec *pvec)
{
	char *pos_mem = pclst->cluster_pos_mem;
	char * record_ptr = pos_mem + ((unsigned long long )(long)(void*)pvec - (unsigned long long)(long)(void*)pclst->cluster_vec_mem);
	struct spt_vec_debug_info *debug = record_ptr;
	return debug->pos;
}
void add_hash_type_record(struct cluster_head_t *pclst, struct spt_vec *pvec,int type)
{
	char *pos_mem = pclst->cluster_pos_mem;
	char * record_ptr = pos_mem + ((unsigned long long )(long)(void*)pvec - (unsigned long long)(long)(void*)pclst->cluster_vec_mem);
	struct spt_vec_debug_info *debug = record_ptr;
	debug->hash_type = type;
}

int get_hash_type_record(struct cluster_head_t *pclst, struct spt_vec *pvec)
{
	char *pos_mem = pclst->cluster_pos_mem;
	char * record_ptr = pos_mem + ((unsigned long long )(long)(void*)pvec - (unsigned long long)(long)(void*)pclst->cluster_vec_mem);
	struct spt_vec_debug_info *debug = record_ptr;
	return debug->hash_type;
}

struct spt_hash_stat_info *hash_stat_ptr;
int hash_stat_switch = 0;
extern unsigned long long data_set_config_map_address;
int hash_stat_find_err_cnt = 10;
void hash_stat_init(void)
{
	hash_stat_ptr = spt_malloc(sizeof(struct spt_hash_stat_info)*4000000);
	if (hash_stat_ptr)
		memset(hash_stat_ptr, 0, sizeof(struct spt_hash_stat_info)*4000000);
	else
		spt_assert(0);
}

void hash_stat_add_hang_hash(char *pdata)
{
	int data_id = ((unsigned long long )(void*)pdata - data_set_config_map_address)/256;
	if (hash_stat_switch)
		hash_stat_ptr[data_id].hang_vec_cnt++;
}

void show_hash_stat_record(void)
{
	int i, hash_data, hash_cnt,hash_cnt_max;
	hash_cnt_max = hash_data = hash_cnt = 0;

	for (i = 0; i < 4000000; i++)
	{
		if (hash_stat_ptr[i].hang_vec_cnt)
			hash_data++;
		if (hash_stat_ptr[i].hang_vec_cnt > hash_cnt_max)
			hash_cnt_max = hash_stat_ptr[i].hang_vec_cnt;
		hash_cnt += hash_stat_ptr[i].hang_vec_cnt;
	}
	printf("hash data cnt is %d, hash vec cnt is %d, hash cnt max is %d\r\n", hash_data, hash_cnt, hash_cnt_max);
}
void show_hash_stat_find_err(void)
{
	int i, hash_data, hash_cnt,hash_cnt_max;
	
	for (i = 0; i < 4000000; i++)
	{
		if (hash_stat_ptr[i].hang_vec_cnt && hash_stat_ptr[i].find_err)
		{
			if (hash_stat_find_err_cnt--)
				printf("find err data 0x%llx,has hang vec\r\n", data_set_config_map_address + i *256);	
			if (!hash_stat_find_err_cnt)
				break;
		}
		
	}
}
void hash_stat_find_err(char *pdata)
{
	int data_id = ((unsigned long long )(void*)pdata - data_set_config_map_address)/256;
	hash_stat_ptr[data_id].find_err++;
}

int last_seg_hash_grp;
int last_window_hash;
#if 0 
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
		fs = find_next_bit(&allocmap, VEC_PER_GRP, fs);
		if (fs >= VEC_PER_GRP) {
			next_grp = grp->next_grp;
			if ((next_grp == 0) || (next_grp == 0xFFFFF))
				return -1;
			gid = next_grp;
			goto re_find;
		}
		pvec = (char *) grp + sizeof(struct spt_grp) + fs *sizeof(struct spt_vec); 
		cur_vec.val = pvec->val;
		PERF_STAT_START(scan_grp_vec_cnt);
		PERF_STAT_END(scan_grp_vec_cnt);
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

void scan_grp_vec(struct spt_grp *grp, int window_hash)
{
	int i = 0;
	struct spt_vec *pvec, cur_vec, *vec;
	vec = NULL;
	for (i = 0 ; i< VEC_PER_GRP; i++) {
		pvec = (char *) grp + sizeof(struct spt_grp) + i *sizeof(struct spt_vec); 
		cur_vec.val = pvec->val;
		if(spt_get_pos_hash(cur_vec) == window_hash) {
			if ((cur_vec.scan_status == SPT_VEC_HVALUE) && (cur_vec.status == SPT_VEC_VALID)) {
				if (vec == NULL) {
					vec = pvec;
				} else {
					if (spt_get_pos_offset(cur_vec) > spt_get_pos_offset(*vec)) {
						vec = pvec;
					}
				}
			}
		}
	}
}
int scan_vec_cnt;
int scan_vec_hash_cnt;
int scan_vec_status_cnt;

unsigned long long scan_grp_vec_cycle1;
unsigned long long scan_grp_vec_cycle2;

int find_start_vec(struct cluster_head_t *pclst, struct spt_vec **vec, int *start_pos, char *data, int window)
{
	int gid,fs;
	struct spt_grp *grp, *next_grp;
	struct spt_pg_h *spt_pg;
	struct spt_vec cur_vec, *pvec, tmp_vec;
	int ret_vec_id = -1;
	unsigned long long  time_begin,time_end;
	int base_vec_id;
	unsigned int window_hash, seg_hash;
	
	//PERF_STAT_START(calc_hash_start_vec);
	calc_hash(data, &window_hash, &seg_hash, window);
	//PERF_STAT_END(calc_hash_start_vec);

	window_hash = (window_hash & SPT_HASH_MASK) << 12;	
	gid = seg_hash %GRP_SPILL_START;
	*start_pos = window *8;
	*vec = NULL;
	tmp_vec.val = 0;
re_find:
	spt_pg = get_vec_pg_head(pclst, gid/GRPS_PER_PG);
	grp = get_grp_from_page_head(spt_pg, gid);
	
	//__builtin_prefetch(grp);
	scan_vec_cnt++;
	pvec = (char *) grp + sizeof(struct spt_grp); 
	base_vec_id = gid << 4;

	//barrier_nospec();	
	//time_begin= rdtsc();

	for (fs = 0 ; fs < VEC_PER_GRP; fs++, pvec++) {
		cur_vec.val = pvec->val & 0x00000000003FFFE3ULL; 	
			
		if(likely((cur_vec.val & 0x00000000003FF000ULL) != window_hash)) 
			continue;
#if 1	
		if ((cur_vec.val & 0x0000000000000021ULL) == 0x0000000000000020ULL) { 
			if (cur_vec.val > tmp_vec.val) {
				tmp_vec.val = cur_vec.val;
				*vec = pvec;
				ret_vec_id = base_vec_id + fs;
			}
		} else {
			if(cur_vec.status == SPT_VEC_DB) {
				fs++;
				pvec++;
			}
		}

#endif
	}
	//barrier_nospec();
	//time_end = rdtsc();
	//scan_grp_vec_cycle1 += time_end- time_begin;
	next_grp = grp->next_grp;
	if ((next_grp == 0) || (next_grp == 0xFFFFF)) {
		return ret_vec_id;
	}
	gid = next_grp;
	goto re_find;
}
#endif
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
