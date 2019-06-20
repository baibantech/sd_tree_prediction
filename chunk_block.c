/*
 * This file is provided under a dual BSD/GPLv2 license. When using or
 * redistributing this file, you may do so under either license.
 *
 * GPL LICENSE SUMMARY
 * Copyright(c) 2016 Baibantech Corporation.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * BSD LICENSE
 * Copyright(c) 2016 Baibantech Corporation.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in
 * the documentation and/or other materials provided with the
 * distribution.
 * * Neither the name of Intel Corporation nor the names of its
 * contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * sd-tree vector and data block management
 *
 * Contact Information:
 * info-linux <info@baibantech.com.cn>
 */

#include "chunk.h"
int g_data_size;

void grp_init_per_page(char *page)
{
	int i;
	struct spt_grp *pgrp;
	for (i = 0 ; i < GRPS_PER_PG; i++) {
		pgrp =(struct spt_grp *) (page + GRP_SIZE*i);
		pgrp->val = 0;
		pgrp->control = 0;
		pgrp->allocmap = 0xFFFCull;
	}
}
void db_grp_init_per_page(char *page)
{
	int i;
	struct spt_grp *pgrp;
	for (i = 0 ; i < GRPS_PER_PG; i++) {
		pgrp =(struct spt_grp *) (page + GRP_SIZE*i);
		pgrp->val = 0;
		pgrp->control = 0;
		pgrp->allocmap = 0x1FCull;
	}
}
int calBitNum(int num)  
{  
    int numOnes = 0;  
  
    while (num != 0) {  
		num &= (num - 1);  
        numOnes++;  
    }  
    return numOnes;  
  
}
char* vec_id_2_ptr(struct cluster_head_t * pclst,unsigned int id)
{
	return (char*)vec_grp_id_2_ptr(pclst, id >> VEC_PER_GRP_BITS) + ((id&VEC_PER_GRP_MASK) << VEC_BITS);
}

char* db_id_2_ptr(struct cluster_head_t * pclst,unsigned int id)
{
	return (char*)db_grp_id_2_ptr(pclst, id / DB_PER_GRP) + (2 << VEC_BITS) + ((id % DB_PER_GRP) << VEC_BITS) + (DB_PER_GRP << VEC_BITS);
}
char* db_ref_id_2_ptr(struct cluster_head_t * pclst,unsigned int id)
{
	return (char*)db_ref_grp_id_2_ptr(pclst, id / DB_PER_GRP) + (2 << VEC_BITS) + ((id % DB_PER_GRP) << VEC_BITS);
}


#if 0
char *vec_grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id)
{
	return cluster_grp_id_2_ptr(pclst, pclst->pglist_vec, grp_id);
}

char *db_grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id)
{
	return cluster_grp_id_2_ptr(pclst, pclst->pglist_db, grp_id);
}
#else
char *vec_grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id)
{
	char *page  = pclst->cluster_vec_mem + ((grp_id >> GRPS_PER_PG_BITS) << PG_BITS);
	return page + ((grp_id&GRPS_PER_PG_MASK) << GRP_BITS);
}

char *db_grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id)
{
	char *page  = pclst->cluster_db_mem + ((grp_id >> GRPS_PER_PG_BITS) << PG_BITS);
	return page + ((grp_id&GRPS_PER_PG_MASK) << GRP_BITS);
}

char *db_ref_grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id)
{
	char *page  = pclst->cluster_db_mem + ((grp_id >> GRPS_PER_PG_BITS) << PG_BITS);
	return page + ((grp_id&GRPS_PER_PG_MASK) << GRP_BITS);
}
#endif
void set_data_size(int size)
{
	g_data_size = size;
}
void free_data(char *p)
{
	spt_free(p);
}
void default_end_get_key(char *p)
{
}
/**
 * upper_get_key - call back function called by up-level
 * cluster to access the data
 * @pdata: pointer of data in the cluster leaf node
 *
 * return the address of the actual data address
 */
char *upper_get_key(char *pdata)
{
	struct spt_dh_ext *ext_head;

	ext_head = (struct spt_dh_ext *)pdata;
	return ext_head->data;
}

void cluster_destroy(struct cluster_head_t *pclst)
{
	if (pclst->is_bottom)
		list_del(&pclst->c_list);

	spt_debug("\r\n");
}


struct cluster_head_t *cluster_init(int is_bottom,
		u64 startbit,
		u64 endbit,
		int thread_num,
		spt_cb_get_key pf,
		spt_cb_end_key pf2,
		spt_cb_free pf_free,
		spt_cb_construct pf_con)
{
	struct cluster_head_t *phead;
	int ptr_bits, i;
	u32 vec;
	struct spt_vec *pvec;
	int pg_num = 0;

	phead = (struct cluster_head_t *)spt_alloc_zero_page();
	if (phead == NULL)
		return 0;

	memset(phead, 0, 4096);

	if (sizeof(char *) == 4)
		ptr_bits = 2;
	if (sizeof(char *) == 8)
		ptr_bits = 3;

	phead->address_info.pg_num_max = CLST_PG_NUM_MAX;
	phead->address_info.pg_ptr_bits = PG_BITS - ptr_bits;

	phead->debug = 0;
	phead->is_bottom = is_bottom;
	phead->startbit = startbit;
	phead->endbit = endbit;
	//phead->thrd_total = thread_num;
	phead->freedata = pf_free;
	phead->construct_data = pf_con;
	phead->spill_grp_id = GRP_SPILL_START;
	phead->spill_db_grp_id = GRP_SPILL_START;
	
	if (is_bottom) {
		phead->get_key = pf;
		phead->get_key_in_tree = pf;
		phead->get_key_end = pf2;
		phead->get_key_in_tree_end = pf2;
	} else {
		phead->get_key = pf;
		phead->get_key_in_tree = upper_get_key;
		phead->get_key_end = pf2;
		phead->get_key_in_tree_end = default_end_get_key;
	}
	
	spt_vec_debug_info_init(phead);
	phead->cluster_vec_mem = spt_malloc(CLST_PG_NUM_MAX << PG_BITS);
	phead->cluster_db_mem = spt_malloc(CLST_PG_NUM_MAX <<PG_BITS);
	phead->cluster_pos_mem = spt_malloc(CLST_PG_NUM_MAX <<PG_BITS);
	for (i = 0 ; i < CLST_PG_NUM_MAX; i++) {
		grp_init_per_page(phead->cluster_vec_mem + (i << PG_BITS));	
		db_grp_init_per_page(phead->cluster_db_mem + (i << PG_BITS));	
		grp_init_per_page(phead->cluster_pos_mem + (i << PG_BITS));	
	}

    vec = vec_alloc(phead, &pvec, 0);
	if (pvec == 0) {
		spt_free(phead);
		return NULL;
	}
	phead->last_alloc_id = vec;
	pvec->val = 0;
	pvec->type = SPT_VEC_DATA;
	pvec->pos = startbit - 1;
	add_real_pos_record(phead, pvec, startbit);
	pvec->scan_status = SPT_VEC_PVALUE;
	pvec->down = SPT_NULL;
	pvec->rd = SPT_NULL;

	phead->vec_head = vec;
	phead->pstart = pvec;

	return phead;
}

void db_free(struct cluster_head_t *pclst, int id)
{
	struct spt_grp *grp;
	struct spt_grp va_old;
	u64 allocmap, freemap;
	int offset;
	u32 tick;
	struct spt_grp st_grp ;
	
	grp  = get_db_grp_from_grpid(pclst, id / DB_PER_GRP);
	offset = id%DB_PER_GRP + 2;

	while(1)
	{
		smp_mb();
		va_old.control = grp->control;
		va_old.val = grp->val;
		smp_mb();
		st_grp = va_old;
		tick = (u32)atomic_read((atomic_t *)&g_thrd_h->tick);
		tick &= GRP_TICK_MASK;
		if(tick - (u32)st_grp.tick >= 2)
		{
			st_grp.allocmap = st_grp.allocmap | st_grp.freemap;
			st_grp.freemap = 1<< offset;
		}
		else
		{
			st_grp.freemap |= 1<< offset;
		}
		if(va_old.val  == atomic64_cmpxchg((atomic64_t *)&grp->val, va_old.val, st_grp.val)){
			do {
				smp_mb();
				va_old.control = grp->control;
				st_grp.control = va_old.control;
				st_grp.tick = tick;
			}while(va_old.control  != atomic64_cmpxchg((atomic64_t *)&grp->control, va_old.control, st_grp.control));
			break;		
		}
	}
	atomic_sub(1, (atomic_t *)&pclst->data_total);
}

void vec_free(struct cluster_head_t *pclst, int vec_id)
{
	struct spt_grp *grp;
	struct spt_grp va_old;
	u64 allocmap, freemap;
	int offset;
	u32 tick;
	struct spt_grp st_grp ;
	
	grp  = get_grp_from_grpid(pclst, vec_id >> VEC_PER_GRP_BITS);
	offset = vec_id&VEC_PER_GRP_MASK;

	while(1)
	{
		smp_mb();
		va_old.control = grp->control;
		va_old.val = grp->val;
		smp_mb();
		st_grp = va_old;
		tick = (u32)atomic_read((atomic_t *)&g_thrd_h->tick);
		tick &= GRP_TICK_MASK;
		if(tick - (u32)st_grp.tick >= 2)
		{
			st_grp.allocmap = st_grp.allocmap | st_grp.freemap;
			st_grp.freemap = 1<<offset;
		}
		else
		{
			st_grp.freemap |= 1<<offset;
		}
		if(va_old.val  == atomic64_cmpxchg((atomic64_t *)&grp->val, va_old.val, st_grp.val)){
			do {
				smp_mb();
				va_old.control = grp->control;
				st_grp.control = va_old.control;
				st_grp.tick = tick;
			}while(va_old.control  != atomic64_cmpxchg((atomic64_t *)&grp->control, va_old.control, st_grp.control));
			break;		
		}
	}
    atomic_sub(1, (atomic_t *)&pclst->used_vec_cnt);
	return;
}

int vec_alloc_spill_grp(struct cluster_head_t *pclst)
{
	int spill_grp_id = 0;
	if ((pclst->spill_grp_id/GRPS_PER_PG) >= pclst->address_info.pg_num_max) {
		printf("cluster %p spill full\r\n", pclst);
		spt_assert(0);
	}
	
	spill_grp_id = atomic_add_return(1, (atomic_t*)&pclst->spill_grp_id);
	spill_grp_id--;
	return spill_grp_id;	
}
int db_alloc_spill_grp(struct cluster_head_t *pclst)
{
	int spill_grp_id = 0;
	if ((pclst->spill_db_grp_id/GRPS_PER_PG) >= pclst->address_info.pg_num_max) {
		printf("cluster %p spill full\r\n", pclst);
		spt_assert(0);
	}
	
	spill_grp_id = atomic_add_return(1, (atomic_t*)&pclst->spill_db_grp_id);
	spill_grp_id--;
	return spill_grp_id;
}

int get_vec_hash_grp_id(struct cluster_head_t *pclst, int vec_id)
{
	int orign_grp = vec_id >> VEC_PER_GRP_BITS;
	int pre_grp_id;
	struct spt_grp *pre_grp, *grp;
	if (orign_grp < GRP_SPILL_START)
		return orign_grp;
	while (orign_grp > GRP_SPILL_START) {
		grp = get_grp_from_grpid(pclst, orign_grp);	
		pre_grp_id = grp->pre_grp;
		orign_grp = pre_grp_id;
	}
	return orign_grp;
}
int vec_judge_preseg_full_and_alloc(struct cluster_head_t *pclst, struct spt_vec **vec, unsigned int sed)
{
	struct spt_grp  *grp, *next_grp;
	int fs, gid, gid_t;
	struct spt_grp old, tmp;
	int cnt = 0;
	u32 tick;
	int alloc_cnt = 0;
	int offset = 0;
	int conflict_cnt = 2;
	gid = sed % GRP_SPILL_START;


re_alloc:
	grp  = get_grp_from_grpid(pclst, gid);
		
    while(1)
    {
		smp_mb();
		old.control = grp->control;
		old.val = grp->val;
		tmp = old;
		if (tmp.freemap !=0) {
			tick = (u32)atomic_read((atomic_t *)&g_thrd_h->tick);
			tick &= GRP_TICK_MASK;
			if(tick - (u32)old.tick >= 2)
			{
				tmp.allocmap = tmp.allocmap | tmp.freemap;
				tmp.freemap = 0;
				if(old.val  == atomic64_cmpxchg((atomic64_t *)&grp->val, old.val, tmp.val)){
					do {
							smp_mb();
							old.control = grp->control;
							tmp.control = old.control;
							tmp.tick = tick;
					}while(old.control  != atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control));
				continue;
				}
			}
		}
		if((old.allocmap & GRP_ALLOCMAP_MASK) == 0)
		{
			if (conflict_cnt) {
				gid = gid + 1;
				conflict_cnt--;
				goto re_alloc;
			}
			return -1;
#if 0
alloc_next_grp:
			offset = begin_offset;
			if (old.next_grp == 0) {
				tmp.next_grp = 0xFFFFF;
				if(old.control == atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control)) {
					next_grp_id = vec_alloc_spill_grp(pclst);
					next_grp  = get_grp_from_grpid(pclst, next_grp_id);
					next_grp->pre_grp = gid;
					do {
						smp_mb();
						old.control = grp->control;
						tmp.next_grp = next_grp_id;
						tmp.resv = 100;
					} while(old.control != atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control)); 
			
					smp_mb();
					gid = grp->next_grp;
					goto re_alloc;
				}
				continue;
			}
			if (old.next_grp == 0xFFFFF)
				continue;
			gid = grp->next_grp;
			if(gid == 0xFFFFF)
				printf("@@@@@@@old.next_grp is %d\r\n",old.next_grp);
			goto re_alloc;
#endif
		}
		
		fs = find_next_bit(&old.val, VEC_PER_GRP,offset);
		if(fs >= VEC_PER_GRP) {
			offset = 0;
			return -1;
			//goto re_alloc;
		}
		tmp.allocmap = old.allocmap & (~(1 << fs));
		
        if(old.val == atomic64_cmpxchg((atomic64_t *)&grp->val, old.val, tmp.val))
            break;
    }
    atomic_add(1, (atomic_t *)&pclst->used_vec_cnt);
	*vec = (struct spt_vec *)((char *)grp + (fs << VEC_BITS));
	return (gid << VEC_PER_GRP_BITS) + fs;
}


int vec_judge_full_and_alloc(struct cluster_head_t *pclst, struct spt_vec **vec, unsigned int sed)
{
	struct spt_grp  *grp, *next_grp;
	int fs, gid, gid_t;
	struct spt_grp old, tmp;
	int cnt = 0;
	u32 tick;
	int alloc_cnt = 0;
	int offset = 0;
	int conflict_cnt = 0;
	gid = sed % GRP_SPILL_START;


re_alloc:
	grp  = get_grp_from_grpid(pclst, gid);
		
    while(1)
    {
		smp_mb();
		old.control = grp->control;
		old.val = grp->val;
		tmp = old;
		if (tmp.freemap !=0) {
			tick = (u32)atomic_read((atomic_t *)&g_thrd_h->tick);
			tick &= GRP_TICK_MASK;
			if(tick - (u32)old.tick >= 2)
			{
				tmp.allocmap = tmp.allocmap | tmp.freemap;
				tmp.freemap = 0;
				if(old.val  == atomic64_cmpxchg((atomic64_t *)&grp->val, old.val, tmp.val)){
					do {
							smp_mb();
							old.control = grp->control;
							tmp.control = old.control;
							tmp.tick = tick;
					}while(old.control  != atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control));
				continue;
				}
			}
		}
		if((old.allocmap & GRP_ALLOCMAP_MASK) == 0)
		{
			if (conflict_cnt) {
				gid = gid + 1;
				conflict_cnt--;
				goto re_alloc;
			}
			return -1;
#if 0
alloc_next_grp:
			offset = begin_offset;
			if (old.next_grp == 0) {
				tmp.next_grp = 0xFFFFF;
				if(old.control == atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control)) {
					next_grp_id = vec_alloc_spill_grp(pclst);
					next_grp  = get_grp_from_grpid(pclst, next_grp_id);
					next_grp->pre_grp = gid;
					do {
						smp_mb();
						old.control = grp->control;
						tmp.next_grp = next_grp_id;
						tmp.resv = 100;
					} while(old.control != atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control)); 
			
					smp_mb();
					gid = grp->next_grp;
					goto re_alloc;
				}
				continue;
			}
			if (old.next_grp == 0xFFFFF)
				continue;
			gid = grp->next_grp;
			if(gid == 0xFFFFF)
				printf("@@@@@@@old.next_grp is %d\r\n",old.next_grp);
			goto re_alloc;
#endif
		}
		
		fs = find_next_bit(&old.val, VEC_PER_GRP,offset);
		if(fs >= VEC_PER_GRP) {
			offset = 0;
			return -1;
			//goto re_alloc;
		}
		tmp.allocmap = old.allocmap & (~(1 << fs));
		
        if(old.val == atomic64_cmpxchg((atomic64_t *)&grp->val, old.val, tmp.val))
            break;
    }
    atomic_add(1, (atomic_t *)&pclst->used_vec_cnt);
	*vec = (struct spt_vec *)((char *)grp + (fs << VEC_BITS));
	return (gid << VEC_PER_GRP_BITS) + fs;
}

int vec_alloc(struct cluster_head_t *pclst,  struct spt_vec **vec, unsigned int sed)
{
	struct spt_grp  *grp, *next_grp;
	int fs, gid, gid_t;
	struct spt_grp old, tmp;
	int cnt = 0;
	int spill_grp_id = 0;
	int alloc_debug = 0;
	int next_grp_id = 0;
	u32 tick;
	int alloc_cnt = 0;
	int begin_offset = (((sed/GRP_SPILL_START)&0x0F)%14) + 2;
	int offset = begin_offset;
	gid = sed % GRP_SPILL_START;


re_alloc:
	grp  = get_grp_from_grpid(pclst, gid);
		
    while(1)
    {
		smp_mb();
		old.control = grp->control;
		old.val = grp->val;
		tmp = old;
		if (tmp.freemap !=0) {
			tick = (u32)atomic_read((atomic_t *)&g_thrd_h->tick);
			tick &= GRP_TICK_MASK;
			if(tick - (u32)old.tick >= 2)
			{
				tmp.allocmap = tmp.allocmap | tmp.freemap;
				tmp.freemap = 0;
				if(old.val  == atomic64_cmpxchg((atomic64_t *)&grp->val, old.val, tmp.val)){
					do {
							smp_mb();
							old.control = grp->control;
							tmp.control = old.control;
							tmp.tick = tick;
					}while(old.control  != atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control));
				continue;
				}
			}
		}
		if((old.allocmap & GRP_ALLOCMAP_MASK) == 0)
		{
alloc_next_grp:
			offset = begin_offset;
			if (old.next_grp == 0) {
				tmp.next_grp = 0xFFFFF;
				if(old.control == atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control)) {
					next_grp_id = vec_alloc_spill_grp(pclst);
					next_grp  = get_grp_from_grpid(pclst, next_grp_id);
					next_grp->pre_grp = gid;
					do {
						smp_mb();
						old.control = grp->control;
						tmp.next_grp = next_grp_id;
						tmp.resv = 100;
					} while(old.control != atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control)); 
			
					smp_mb();
					gid = grp->next_grp;
					goto re_alloc;
				}
				continue;
			}
			if (old.next_grp == 0xFFFFF)
				continue;
			gid = grp->next_grp;
			if(gid == 0xFFFFF)
				printf("@@@@@@@old.next_grp is %d\r\n",old.next_grp);
			goto re_alloc;
		}
		
		fs = find_next_bit(&old.val, VEC_PER_GRP,offset);
		if(fs >= VEC_PER_GRP) {
			offset = 0;
			goto re_alloc;
		}
		tmp.allocmap = old.allocmap & (~(1 << fs));
		
        if(old.val == atomic64_cmpxchg((atomic64_t *)&grp->val, old.val, tmp.val))
            break;
    }
    atomic_add(1, (atomic_t *)&pclst->used_vec_cnt);
	*vec = (struct spt_vec *)((char *)grp + (fs << VEC_BITS));
	return (gid << VEC_PER_GRP_BITS) + fs;
}

unsigned int db_alloc(struct cluster_head_t *pclst,  struct spt_dh **db, struct spt_dh_ref **ref, unsigned int hash)
{
	struct spt_grp  *grp, *next_grp;
	int fs, gid, gid_t;
	struct spt_grp old, tmp;
	int cnt = 0;
	int spill_grp_id = 0;
	int alloc_debug = 0;
	int next_grp_id = 0;
	u32 tick;
	gid = hash % GRP_SPILL_START;

re_alloc:
	grp  = get_db_grp_from_grpid(pclst, gid);
		
    while(1)
    {
		smp_mb();
		old.control = grp->control;
		old.val = grp->val;
		tmp = old;
		if (tmp.freemap != 0) {
			tick = (u32)atomic_read((atomic_t *)&g_thrd_h->tick);
			tick &= GRP_TICK_MASK;
			if(tick - (u32)old.tick >= 2)
			{
				tmp.allocmap = tmp.allocmap | tmp.freemap;
				tmp.freemap = 0;
				if(old.val  == atomic64_cmpxchg((atomic64_t *)&grp->val, old.val, tmp.val)){
					do {
							smp_mb();
							old.control = grp->control;
							tmp.control = old.control;
							tmp.tick = tick;
					}while(old.control  != atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control));
				continue;
				}
			}
		}
		if((old.allocmap & GRP_ALLOCMAP_MASK) == 0)
		{
alloc_next_grp:
			if (old.next_grp == 0) {
				tmp.next_grp = 0xFFFFF;
				if(old.control == atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control)) {
					next_grp_id = db_alloc_spill_grp(pclst);
					next_grp  = get_db_grp_from_grpid(pclst, next_grp_id);
					next_grp->pre_grp = gid;
					do {
						smp_mb();
						old.control = grp->control;
						tmp.next_grp = next_grp_id;
						tmp.resv = 100;
					} while(old.control != atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control)); 
			
					smp_mb();
					gid = grp->next_grp;
					goto re_alloc;
				}
				continue;
			}
			if (old.next_grp == 0xFFFFF)
				continue;
			gid = grp->next_grp;
			if(gid == 0xFFFFF)
				printf("@@@@@@@old.next_grp is %d\r\n",old.next_grp);
			goto re_alloc;
		}
		
		fs = find_next_bit(&old.val, VEC_PER_GRP, 0);

		if(fs >= DB_PER_GRP + 2)
			goto re_alloc;
		
		tmp.allocmap = old.allocmap & (~(1 << fs));
		
        if(old.val == atomic64_cmpxchg((atomic64_t *)&grp->val, old.val, tmp.val))
            break;
    }
    atomic_add(1, (atomic_t *)&pclst->data_total);
	*ref = (struct spt_dh_ref *)((char *)grp + (fs << VEC_BITS));
	(*ref)->ref = 0;
	(*ref)->hash = hash;
	*db =(struct spt_dh *)((char *)(*ref) + (DB_PER_GRP << VEC_BITS));
	(*db)->status = 1;
	(*db)->pdata = 0;
	return (gid * DB_PER_GRP) + fs - 2;
}
