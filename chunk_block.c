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
#include "jhash.h"
int g_data_size;

void grp_init_per_page(char *page)
{
	int i;
	struct spt_grp *pgrp;
	for (i = 0 ; i < GRPS_PER_PG; i++) {
		pgrp = (page + GRP_SIZE*i);
		pgrp->val = 0;
		pgrp->control = 0;
		pgrp->allocmap = 0x3FFFFFFFull;
	}
}

int calBitNum(int num)  
{  
    int numOnes = 0;  
  
    while (num != 0)  
    {  

		num &= (num - 1);  
        numOnes++;  

    }  
    return numOnes;  
  
}
#if 0
u32 vec_grp_alloc_map(struct cluster_head_t *pclst)
{
	int grps, grp_id;
	struct spt_grp *pgrp;
	u32 total=0;
	grps = pclst->pg_cursor*GRPS_PER_PG;	
	for(grp_id=0; grp_id < grps; grp_id++)
	{
		pgrp = (struct spt_grp *)vec_grp_id_2_ptr(pclst, grp_id);
		total += 30-calBitNum(pgrp->allocmap);
	}
	return total;
}

u64 debug_alloc_map()
{
    struct list_head *list_itr;
    struct cluster_head_t *pclst;
    int i=0;
	u32 bittotal;

    list_for_each(list_itr, &pgclst->c_list)
    {
        pclst = list_entry(list_itr, struct cluster_head_t, c_list);
		bittotal = vec_grp_alloc_map(pclst);
        spt_print("[cluster %d]",i);
		spt_print("%p [data_used]:%d [vec_used]:%d [bit_used]:%d \r\n", 
		pclst, pclst->data_total, pclst->used_vec_cnt, bittotal);
        i++;
    }
}

void vec_grp_free_map(struct cluster_head_t *pclst)
{
	int grps, grp_id;
	struct spt_grp *pgrp, st_grp;
	u32 total=0;
	u64 va_old;
	u32 tick;
	
	grps = pclst->pg_cursor*GRPS_PER_PG;	
	for(grp_id=0; grp_id < grps; grp_id++)
	{
		pgrp = (struct spt_grp *)vec_grp_id_2_ptr(pclst, grp_id);
		while(1)
		{
			va_old = atomic64_read((atomic64_t *)pgrp);
			st_grp.val = va_old;
			tick = (u32)atomic_read((atomic_t *)&g_thrd_h->tick);
			tick &= GRP_TICK_MASK;
			if((tick > st_grp.tick && tick - (u32)st_grp.tick >= 2)
				|| (tick < st_grp.tick && tick + GRP_TICK_MASK - (u32)st_grp.tick >= 2))
			{
				st_grp.allocmap = st_grp.allocmap | st_grp.freemap;
				st_grp.freemap = 0;
			}
			st_grp.tick = tick;
			if(va_old  == atomic64_cmpxchg((atomic64_t *)pgrp, va_old, st_grp.val))
				break;		
		}
	}
}

void debug_free_all()
{
    struct list_head *list_itr;
    struct cluster_head_t *pclst;
    int i;
	u32 bittotal;

    list_for_each(list_itr, &pgclst->c_list)
    {
        pclst = list_entry(list_itr, struct cluster_head_t, c_list);
		vec_grp_free_map(pclst);
    }
	return;
}
#endif
char* vec_id_2_ptr(struct cluster_head_t * pclst,unsigned int id)
{
	return (char*)vec_grp_id_2_ptr(pclst, id/VEC_PER_GRP) + sizeof(struct spt_grp) + id%VEC_PER_GRP*VBLK_SIZE;
}

char* db_id_2_ptr(struct cluster_head_t * pclst,unsigned int id)
{
	return (char*)db_grp_id_2_ptr(pclst, id/VEC_PER_GRP) + sizeof(struct spt_grp)+ id%VEC_PER_GRP*VBLK_SIZE;
}



/**
 * get_pg_head - called by db_id_2_ptr & vec_id_2_ptr
 * @pclst: pointer of sd tree cluster head
 * @id: page id in this cluster
 * return the address of page head
 */
struct spt_pg_h  *get_vec_pg_head(struct cluster_head_t *pclst, unsigned int id)
{
	int ptrs_bit = pclst->pg_ptr_bits;
	int ptrs = (1 << ptrs_bit);
	u64 direct_pgs = CLST_NDIR_PGS;
	u64 indirect_pgs = 1<<ptrs_bit;
	u64 double_pgs = 1<<(ptrs_bit*2);
	int pg_id = id ;
	int offset;
	int get_cnt= 0;
	char *page, **indir_page, ***dindir_page;
get_page_id:
	get_cnt++;
	if (id < direct_pgs) {
		page = (char *)pclst->pglist_vec[id];
		if (page)
			goto ret_id;

	} else if ((id - direct_pgs) < indirect_pgs) {
		pg_id = id - direct_pgs;
		indir_page = (char **)pclst->pglist_vec[CLST_IND_PG];
		if (indir_page != NULL) {
			offset = pg_id;
			page = indir_page[offset];
			if (page)
				goto ret_id;
		}
	} else if ((id - direct_pgs -  indirect_pgs) < double_pgs) {
		pg_id = id - direct_pgs -  indirect_pgs;
		dindir_page = (char ***)pclst->pglist_vec[CLST_DIND_PG];
		if (dindir_page != NULL) {
			offset = pg_id >> ptrs_bit;
			indir_page = dindir_page[offset];
			if (indir_page != 0) {
				offset = pg_id & (ptrs-1);
				page = indir_page[offset];
				if (page)
					goto ret_id;
			}
		}
	} else {
		spt_debug("warning: id is too big\r\n");
		return NULL;
	}
	cluster_vec_add_page(pclst, id);
	goto get_page_id;
ret_id:
	return page + PG_HEAD_OFFSET;
}
struct spt_pg_h  *get_db_pg_head(struct cluster_head_t *pclst, unsigned int id)
{
	int ptrs_bit = pclst->pg_ptr_bits;
	int ptrs = (1 << ptrs_bit);
	u64 direct_pgs = CLST_NDIR_PGS;
	u64 indirect_pgs = 1<<ptrs_bit;
	u64 double_pgs = 1<<(ptrs_bit*2);
	int pg_id = id ;
	int offset;
	int get_cnt= 0;
	char *page, **indir_page, ***dindir_page;
get_page_id:
	get_cnt++;
	if (id < direct_pgs) {
		page = (char *)pclst->pglist_db[id];
		if (page)
			goto ret_id;

	} else if ((id - direct_pgs) < indirect_pgs) {
		pg_id = id - direct_pgs;
		indir_page = (char **)pclst->pglist_db[CLST_IND_PG];
		if (indir_page != NULL) {
			offset = pg_id;
			page = indir_page[offset];
			if (page)
				goto ret_id;
		}
	} else if ((id - direct_pgs -  indirect_pgs) < double_pgs) {
		pg_id = id - direct_pgs -  indirect_pgs;
		dindir_page = (char ***)pclst->pglist_db[CLST_DIND_PG];
		if (dindir_page != NULL) {
			offset = pg_id >> ptrs_bit;
			indir_page = dindir_page[offset];
			if (indir_page != 0) {
				offset = pg_id & (ptrs-1);
				page = indir_page[offset];
				if (page)
					goto ret_id;
			}
		}
	} else {
		spt_debug("warning: id is too big\r\n");
		return NULL;
	}
	cluster_db_add_page(pclst, id);
	goto get_page_id;
ret_id:
	return page + PG_HEAD_OFFSET;
}

char *vec_grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id)
{
	int ptrs_bit = pclst->pg_ptr_bits;
	int ptrs = (1 << ptrs_bit);
	u64 direct_pgs = CLST_NDIR_PGS;
	u64 indirect_pgs = 1<<ptrs_bit;
	u64 double_pgs = 1<<(ptrs_bit*2);
	int pg_id = grp_id/GRPS_PER_PG;
	int offset;
	char *page, **indir_page, ***dindir_page;

	if (pg_id < direct_pgs) {
		page = (char *)pclst->pglist_vec[pg_id];
		while (page == 0) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			page = (char *)atomic64_read(
				(atomic64_t *)&pclst->pglist_vec[pg_id]);
		}
	} else if ((pg_id - direct_pgs) < indirect_pgs) {
		pg_id -= direct_pgs;
		indir_page = (char **)pclst->pglist_vec[CLST_IND_PG];
		while (indir_page == NULL) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			indir_page = (char **)atomic64_read(
				(atomic64_t *)&pclst->pglist_vec[CLST_IND_PG]);
		}
		offset = pg_id;
		page = indir_page[offset];
		while (page == 0) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			page = (char *)atomic64_read(
				(atomic64_t *)&indir_page[offset]);
		}
	} else if ((pg_id - direct_pgs -  indirect_pgs) < double_pgs) {
		pg_id = pg_id - direct_pgs -  indirect_pgs;
		dindir_page = (char ***)pclst->pglist_vec[CLST_DIND_PG];
		while (dindir_page == NULL) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			dindir_page = (char ***)atomic64_read(
				(atomic64_t *)&pclst->pglist_vec[CLST_DIND_PG]);
		}
		offset = pg_id >> ptrs_bit;
		indir_page = dindir_page[offset];
		while (indir_page == 0) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			indir_page = (char **)atomic64_read(
				(atomic64_t *)&dindir_page[offset]);
		}
		offset = pg_id & (ptrs-1);
		page = indir_page[offset];
		while (page == 0) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			page = (char *)atomic64_read(
			(atomic64_t *)&indir_page[offset]);
		}
	} else {
		spt_debug("warning: id is too big\r\n");
		return 0;
	}
	offset = (grp_id%GRPS_PER_PG) * GRP_SIZE;
	return page + offset;
}

char *db_grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id)
{
	int ptrs_bit = pclst->pg_ptr_bits;
	int ptrs = (1 << ptrs_bit);
	u64 direct_pgs = CLST_NDIR_PGS;
	u64 indirect_pgs = 1<<ptrs_bit;
	u64 double_pgs = 1<<(ptrs_bit*2);
	int pg_id = grp_id/GRPS_PER_PG;
	int offset;
	char *page, **indir_page, ***dindir_page;

	if (pg_id < direct_pgs) {
		page = (char *)pclst->pglist_db[pg_id];
		while (page == 0) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			page = (char *)atomic64_read(
				(atomic64_t *)&pclst->pglist_db[pg_id]);
		}
	} else if ((pg_id - direct_pgs) < indirect_pgs) {
		pg_id -= direct_pgs;
		indir_page = (char **)pclst->pglist_db[CLST_IND_PG];
		while (indir_page == NULL) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			indir_page = (char **)atomic64_read(
				(atomic64_t *)&pclst->pglist_db[CLST_IND_PG]);
		}
		offset = pg_id;
		page = indir_page[offset];
		while (page == 0) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			page = (char *)atomic64_read(
				(atomic64_t *)&indir_page[offset]);
		}
	} else if ((pg_id - direct_pgs -  indirect_pgs) < double_pgs) {
		pg_id = pg_id - direct_pgs -  indirect_pgs;
		dindir_page = (char ***)pclst->pglist_db[CLST_DIND_PG];
		while (dindir_page == NULL) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			dindir_page = (char ***)atomic64_read(
				(atomic64_t *)&pclst->pglist_db[CLST_DIND_PG]);
		}
		offset = pg_id >> ptrs_bit;
		indir_page = dindir_page[offset];
		while (indir_page == 0) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			indir_page = (char **)atomic64_read(
				(atomic64_t *)&dindir_page[offset]);
		}
		offset = pg_id & (ptrs-1);
		page = indir_page[offset];
		while (page == 0) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			page = (char *)atomic64_read(
			(atomic64_t *)&indir_page[offset]);
		}
	} else {
		spt_debug("warning: id is too big\r\n");
		return 0;
	}
	offset = (grp_id%GRPS_PER_PG) * GRP_SIZE;
	return page + offset;
}

struct spt_grp *get_grp_from_page_head(char *page_head, unsigned int grp_id)
{
	return (page_head - PG_HEAD_OFFSET)+ (grp_id%GRPS_PER_PG) * GRP_SIZE;
}

void cluster_vec_add_page(struct cluster_head_t *pclst, int pg_id)	
{
	char *page, **indir_page, ***dindir_page;
	u32 old_head;
	int i, total, id, offset, offset2;
	struct block_head_t *blk;
	int ptrs_bit = pclst->pg_ptr_bits;
	int ptrs = (1 << ptrs_bit);
	u64 direct_pgs = CLST_NDIR_PGS;
	u64 indirect_pgs = 1<<ptrs_bit;
	u64 double_pgs = 1<<(ptrs_bit*2);
	u64 old_pg;

	if (pclst->pg_num_max <= pg_id) {
		return;
	}
	page = spt_alloc_zero_page();
	if (page == NULL)
		return;

	grp_init_per_page(page);
	id = pg_id;
	if (id < direct_pgs) {
		old_pg = pclst->pglist_vec[id];
		if (old_pg == 0)
			old_pg = atomic64_cmpxchg((atomic64_t*)&pclst->pglist_vec[id],
				   old_pg, (u64)page);
		if (old_pg)
			spt_free(page);
	} else if ((id - direct_pgs) < indirect_pgs) {
		id -= direct_pgs;
		offset = id;
		old_pg = pclst->pglist_vec[CLST_IND_PG];
		if (old_pg == 0){
			indir_page = (char **)spt_alloc_zero_page();
			if (indir_page == NULL)
				return ;
			old_pg = atomic64_cmpxchg((atomic64_t*)&pclst->pglist_vec[CLST_IND_PG],
					(u64)old_pg, (u64)indir_page);
		
			if (old_pg){
				spt_free(page);
				spt_free(indir_page);
				return;
			}
		}
		indir_page = (char **)pclst->pglist_vec[CLST_IND_PG];
		old_pg = indir_page[offset];
		if (old_pg == 0)
			old_pg = atomic64_cmpxchg((atomic64_t *)&indir_page[offset],
					(u64)old_pg, (u64)page);
	
		if (old_pg)
			spt_free(page);
	} else if ((id - direct_pgs - indirect_pgs) < double_pgs) {
		id = id - direct_pgs - indirect_pgs;
		offset = id >> ptrs_bit;
		offset2 = id & (ptrs-1);

		old_pg = pclst->pglist_vec[CLST_DIND_PG];
		if (old_pg == 0) {
			dindir_page = (char ***)spt_alloc_zero_page();
			if (dindir_page == NULL)
				return ;
			old_pg = atomic64_cmpxchg((atomic64_t*)&pclst->pglist_vec[CLST_DIND_PG],
					(u64)old_pg, (u64)dindir_page);
			if (old_pg) {
				spt_free(dindir_page);
				spt_free(page);
				return;
			}
		}
		
		dindir_page = (char ***)pclst->pglist_vec[CLST_DIND_PG];

		old_pg = dindir_page[offset];

		if (old_pg == 0) {
			indir_page = (char **)spt_alloc_zero_page();
			if (indir_page == NULL)
				return;
			old_pg = atomic64_cmpxchg((atomic64_t *)&dindir_page[offset],
					(u64)old_pg, (u64)indir_page);
			if (old_pg) {
				spt_free(indir_page);
				spt_free(page);
				return;
			}
		}
		indir_page = dindir_page[offset];

		old_pg = indir_page[offset2];
		if (old_pg == 0)
			old_pg = atomic64_cmpxchg((atomic64_t *)&indir_page[offset2],
				   old_pg, (u64)page);
		if (old_pg)
			spt_free(page);
	} else {
		spt_debug("warning: id is too big");
		return ;
	}
	return;
}
void cluster_db_add_page(struct cluster_head_t *pclst, int pg_id)	
{
	char *page, **indir_page, ***dindir_page;
	u32 old_head;
	int i, total, id, offset, offset2;
	struct block_head_t *blk;
	int ptrs_bit = pclst->pg_ptr_bits;
	int ptrs = (1 << ptrs_bit);
	u64 direct_pgs = CLST_NDIR_PGS;
	u64 indirect_pgs = 1<<ptrs_bit;
	u64 double_pgs = 1<<(ptrs_bit*2);
	u64 old_pg;

	if (pclst->pg_num_max <= pg_id) {
		return;
	}
	page = spt_alloc_zero_page();
	if (page == NULL)
		return;

	grp_init_per_page(page);
	id = pg_id;
	if (id < direct_pgs) {
		old_pg = pclst->pglist_db[id];
		if (old_pg == 0)
			old_pg = atomic64_cmpxchg((atomic64_t*)&pclst->pglist_db[id],
				   old_pg, (u64)(void *)page);
		if (old_pg)
			spt_free(page);
	} else if ((id - direct_pgs) < indirect_pgs) {
		id -= direct_pgs;
		offset = id;
		old_pg = pclst->pglist_db[CLST_IND_PG];
		if (old_pg == 0){
			indir_page = (char **)spt_alloc_zero_page();
			if (indir_page == NULL)
				return ;
			old_pg = atomic64_cmpxchg((atomic64_t*)&pclst->pglist_db[CLST_IND_PG],
					(u64)old_pg, (u64)indir_page);
		
			if (old_pg){
				spt_free(page);
				spt_free(indir_page);
				return;
			}
		}
		indir_page = (char **)pclst->pglist_db[CLST_IND_PG];
		old_pg = indir_page[offset];
		if (old_pg == 0)
			old_pg = atomic64_cmpxchg((atomic64_t *)&indir_page[offset],
					(u64)old_pg, (u64)page);
	
		if (old_pg)
			spt_free(page);
	} else if ((id - direct_pgs - indirect_pgs) < double_pgs) {
		id = id - direct_pgs - indirect_pgs;
		offset = id >> ptrs_bit;
		offset2 = id & (ptrs-1);

		old_pg = pclst->pglist_db[CLST_DIND_PG];
		if (old_pg == 0){
			dindir_page = (char ***)spt_alloc_zero_page();
			if (dindir_page == NULL)
				return ;
			old_pg = atomic64_cmpxchg((atomic64_t*)&pclst->pglist_db[CLST_DIND_PG],
					(u64)old_pg, (u64)dindir_page);
			if (old_pg) {
				spt_free(dindir_page);
				spt_free(page);
				return;
			}
		}
		
		dindir_page = (char ***)pclst->pglist_db[CLST_DIND_PG];

		old_pg = dindir_page[offset];

		if (old_pg == 0) {
			indir_page = (char **)spt_alloc_zero_page();
			if (indir_page == NULL)
				return;
			old_pg = atomic64_cmpxchg((atomic64_t *)&dindir_page[offset],
					(u64)old_pg, (u64)indir_page);
			if (old_pg) {
				spt_free(indir_page);
				spt_free(page);
				return;
			}
		}
		indir_page = dindir_page[offset];

		old_pg = indir_page[offset2];
		if (old_pg == 0)
			old_pg = atomic64_cmpxchg((atomic64_t *)&indir_page[offset2],
				   old_pg, (u64)page);
		if (old_pg)
			spt_free(page);
	} else {
		spt_debug("warning: id is too big");
		return ;
	}
	return;
}

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

	phead = (struct cluster_head_t *)spt_alloc_zero_page();
	if (phead == NULL)
		return 0;

	memset(phead, 0, 4096);

	if (sizeof(char *) == 4)
		ptr_bits = 2;
	if (sizeof(char *) == 8)
		ptr_bits = 3;
	phead->pg_num_max = CLST_PG_NUM_MAX;
	phead->pg_ptr_bits = PG_BITS - ptr_bits;

	phead->debug = 1;
	phead->is_bottom = is_bottom;
	phead->startbit = startbit;
	phead->endbit = endbit;
	phead->thrd_total = thread_num;
	phead->freedata = pf_free;
	phead->construct_data = pf_con;
	phead->spill_grp_id = GRP_SPILL_START;
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
	
	phead->pglist_db = spt_alloc_zero_page();

	if (!phead->pglist_db) {
		spt_free(phead);
		return NULL;
	}


    vec = vec_alloc_from_grp(phead, 0, &pvec,__LINE__);
	if (pvec == 0) {
		spt_free(phead);
		spt_free(phead->pglist_db);
		return NULL;
	}
	phead->last_alloc_id = vec;
	pvec->val = 0;
	pvec->type = SPT_VEC_DATA;
	pvec->pos = startbit - 1;
	pvec->down = SPT_NULL;
	pvec->rd = SPT_NULL;
	phead->vec_head = vec;
	phead->pstart = pvec;

	return phead;
}
u64 g_spill_vec = 0;
void test_break(void)
{
	printf("test_break\r\n");
}
int vec_alloc_from_spill(struct cluster_head_t *pclst, struct spt_vec **vec, int pgid)
{
	struct spt_grp *grp;
	int fs, gid, i;
	struct spt_pg_h *spt_pg;
	struct spt_grp va_old,va_new;

	atomic64_add(1, (atomic64_t *)&g_spill_vec);
	pgid++;
	if(pgid >= pclst->pg_num_max) {
		*vec = NULL;
		return SPT_NULL;
	}
	while(1)
	{
		spt_pg = get_vec_pg_head(pclst, pgid);
		if(spt_pg->bit_used < PG_SPILL_WATER_MARK)
		{
			gid = pgid*GRPS_PER_PG;
			for(i=0;i<GRPS_PER_PG;i++)
			{
				grp  = get_grp_from_page_head(spt_pg, gid);
				while(1)
				{
					va_old = *grp;
					if((va_old.allocmap & GRP_ALLOCMAP_MASK) == 0)
						break;
					
					fs = find_next_bit(&va_old.val, 32, 0);
					if(fs >=32)
						break;
					
					va_new = va_old;
					va_new.allocmap = va_old.allocmap & (~(1 << fs));
					
					if(va_old.val == atomic64_cmpxchg((atomic64_t *)&grp->val, va_old.val, va_new.val))
					{
						atomic_sub(1, (atomic_t *)&pclst->free_vec_cnt);
						atomic_add(1, (atomic_t *)&pclst->used_vec_cnt);
						atomic_add(1, (atomic_t *)&spt_pg->bit_used);
						*vec = (struct spt_vec *)((char*)grp + sizeof(struct spt_grp) + fs*sizeof(struct spt_vec));
						//printf("cluster %p, vec alloc spill id %d\r\n",pclst, gid*VEC_PER_GRP +fs);
						return gid*VEC_PER_GRP + fs;
					}
				}
				gid++;
			}	
		}
		pgid++;
		if(pclst->pg_num_max <= pgid)
		{
			*vec = NULL;
			return SPT_NULL;
		}
	}
}


int db_alloc_from_spill(struct cluster_head_t *pclst, struct spt_dh **db, int pgid)
{
	struct spt_grp *grp;
	int fs, ns, gid , i, retry;
	struct spt_pg_h *spt_pg;
	struct spt_grp va_old, va_new;

	retry=0;
	pgid++;
	
	if(pclst->pg_num_max <= pgid) {
		*db = NULL;
		return SPT_NULL;
	}
	while(1)
	{
		spt_pg = get_db_pg_head(pclst, pgid);
		if(spt_pg->bit_used < PG_SPILL_WATER_MARK)
		{
			gid = pgid*GRPS_PER_PG;
			for(i=0;i<GRPS_PER_PG;i++)
			{
				grp  = get_grp_from_page_head(spt_pg, gid);
				while(1)
				{
					va_old = *grp;
					if((va_old.allocmap & GRP_ALLOCMAP_MASK) == 0)
						break;
					
					fs = find_next_bit(&va_old.val, 32, 0);
					while(1) {
						if(fs >= 31)
							break;
						
						ns = find_next_bit(&va_old.val, 32, fs+1);
						if(ns == fs+1) {
							va_new = va_old;
							va_new.allocmap  = va_old.allocmap & (~(3 << fs));
							
							if(va_old.val  == atomic64_cmpxchg((atomic64_t *)&grp->val, va_old.val,va_new.val)) {
								atomic_add(1, (atomic_t *)&pclst->data_total);
								*db = (struct spt_dh *)((char *)grp + sizeof(struct spt_grp) + fs*sizeof(struct spt_vec));
								atomic_add(2, (atomic_t *)&spt_pg->bit_used);
								(*db)->rsv = 0;
								(*db)->pdata = NULL;
								//printf("cluster %p, db alloc spill id %d\r\n",pclst, gid*VEC_PER_GRP +fs);
								return gid*VEC_PER_GRP + fs;
							} else {
								retry = 1;
								break;
							}
						}
						fs = ns;
					}
					if(retry == 0)
						break;
					else
						retry = 0;
				}
				gid++;
			}	
		}
		pgid++;
		if(pclst->pg_num_max <= pgid)
		{
			*db = NULL;
			return SPT_NULL;
		}
	}
}
int vec_alloc_from_grp(struct cluster_head_t *pclst, int id, struct spt_vec **vec, int line)
{
	struct spt_grp *grp;
	int fs, gid, offset, gid_t;
	struct spt_pg_h *spt_pg;
	struct spt_grp va_old, va_new;
	
	gid = gid_t = id/VEC_PER_GRP;
	offset = id%VEC_PER_GRP;
	//printf("alloc line %d\r\n",line);
re_alloc:
	if(( gid/GRPS_PER_PG) >= pclst->pg_num_max) {
		*vec = NULL;
		return SPT_NULL;
	}
	spt_pg = get_vec_pg_head(pclst, gid/GRPS_PER_PG); /*get page head ,if page null alloc page*/
	grp  = get_grp_from_page_head(spt_pg, gid);
    while(1)
    {
        va_old = *grp;
		if((va_old.allocmap & GRP_ALLOCMAP_MASK) == 0)
		{
			if(gid-gid_t >= GRPS_PER_PG)
			{
				return vec_alloc_from_spill(pclst, vec, gid/GRPS_PER_PG);
			}
			gid++;
			offset=0;
			goto re_alloc;
		}
		fs = find_next_bit(&va_old.val, 32, offset);
		if(fs >= 32)
		{
			if(gid-gid_t >= GRPS_PER_PG)
			{
				return vec_alloc_from_spill(pclst, vec, gid/GRPS_PER_PG);
			}
			gid++;
			offset=0;
			goto re_alloc;
		}
		va_new = va_old;
		va_new.allocmap = va_old.allocmap & (~(1 << fs));
		
        if(va_old.val  == atomic64_cmpxchg((atomic64_t *)&grp->val, va_old.val, va_new.val))
            break;
    }
    atomic_sub(1, (atomic_t *)&pclst->free_vec_cnt);
    atomic_add(1, (atomic_t *)&pclst->used_vec_cnt);
	atomic_add(1, (atomic_t *)&spt_pg->bit_used);
	*vec = (struct spt_vec *)((char *)grp + sizeof(struct spt_grp) + fs*sizeof(struct spt_vec));
	//printf("cluster %p,vec alloc id %d\r\n",pclst, gid*VEC_PER_GRP +fs);
	return gid*VEC_PER_GRP + fs;
}

int db_alloc_from_grp(struct cluster_head_t *pclst, int id, struct spt_dh **db)
{
	struct spt_grp *grp;
	int fs, ns, gid, gid_t, offset;	
	struct spt_pg_h *spt_pg;
	struct spt_grp va_old, va_new;

	gid = gid_t = id/VEC_PER_GRP;
	offset = id%VEC_PER_GRP;
	
re_alloc:
	if(( gid/GRPS_PER_PG) >= pclst->pg_num_max) {
		*db = NULL;
		return SPT_NULL;
	}
	spt_pg = get_db_pg_head(pclst, gid/GRPS_PER_PG);
	grp  = get_grp_from_page_head(spt_pg, gid);
	while(1)
	{
		va_old =  *grp;
		if((va_old.allocmap & GRP_ALLOCMAP_MASK) == 0)
		{
			if(gid-gid_t >= GRPS_PER_PG)
			{
				return db_alloc_from_spill(pclst, db, gid/GRPS_PER_PG);
			}
			gid++;
			offset=0;
			goto re_alloc;
		}
		fs = find_next_bit(&va_old.val, 32, offset);
		while(1)
		{
			if(fs >= 31)
			{
				if(gid-gid_t >= GRPS_PER_PG)
				{
					return db_alloc_from_spill(pclst, db, gid/GRPS_PER_PG);
				}
				gid++;
				offset=0;
				goto re_alloc;
			}
			ns = find_next_bit(&va_old.val, 32, fs+1);
			if(ns == fs+1)
				break;
			fs = ns;
		}
		va_new = va_old;
		va_new.allocmap = va_old.allocmap & (~(3 << fs));
		
		if(va_old.val  == atomic64_cmpxchg((atomic64_t *)&grp->val, va_old.val, va_new.val))
			break;
	}
	atomic_add(1, (atomic_t *)&pclst->data_total);	
	atomic_add(2, (atomic_t *)&spt_pg->bit_used);
	*db = (struct spt_dh *)((char *)grp + sizeof(struct spt_grp) + fs*sizeof(struct spt_vec));
    (*db)->rsv = 0;
    (*db)->pdata = NULL;
	//printf("cluster %p,db alloc id %d\r\n",pclst, gid*VEC_PER_GRP +fs);
	
	return gid*VEC_PER_GRP + fs;
}


void db_free(struct cluster_head_t *pclst, int id)
{
	struct spt_grp *grp;
	struct spt_grp va_old;
	int offset;
	u32 tick;
	struct spt_grp st_grp ;
	struct spt_pg_h *spt_pg;

	//printf("db free id %d\r\n",id);
	spt_pg = get_db_pg_head(pclst, id/VEC_PER_GRP/GRPS_PER_PG);
	grp  = get_grp_from_page_head(spt_pg, id/VEC_PER_GRP);
	offset = id%VEC_PER_GRP;

	while(1)
	{
		va_old = *grp;
		st_grp = va_old;
		tick = (u32)atomic_read((atomic_t *)&g_thrd_h->tick);
		tick &= GRP_TICK_MASK;
		if(tick - (u32)st_grp.tick >= 2)
		{
			st_grp.allocmap = st_grp.allocmap | st_grp.freemap;
			st_grp.freemap = 3<<offset;
		}
		else
		{
			st_grp.freemap |= 3<<offset;
		}
		if(va_old.val  == atomic64_cmpxchg((atomic64_t *)&grp->val, va_old.val, st_grp.val)) {
			do {
				va_old = *grp;
				st_grp.tick = tick;
			}while(va_old.control  != atomic64_cmpxchg((atomic64_t *)&grp->control, va_old.control, st_grp.control));
			break;		
		}
	}
	atomic_sub(1, (atomic_t *)&pclst->data_total);
	atomic_sub(2, (atomic_t *)&spt_pg->bit_used);
	return;
}

void vec_free(struct cluster_head_t *pclst, int id)
{
	struct spt_grp *grp;
	struct spt_grp va_old;
	u64 allocmap, freemap;
	int offset;
	u32 tick;
	struct spt_grp st_grp ;
	struct spt_pg_h *spt_pg;
	//printf("vec free id %d\r\n",id);
	spt_pg = get_vec_pg_head(pclst, id/VEC_PER_GRP/GRPS_PER_PG);
	grp  = get_grp_from_page_head(spt_pg, id/VEC_PER_GRP);
	offset = id%VEC_PER_GRP;

	while(1)
	{
		va_old = *grp;
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
				va_old = *grp;
				st_grp.tick = tick;
			}while(va_old.control  != atomic64_cmpxchg((atomic64_t *)&grp->control, va_old.control, st_grp.control));
			break;		
		}
	}
    atomic_add(1, (atomic_t *)&pclst->free_vec_cnt);
    atomic_sub(1, (atomic_t *)&pclst->used_vec_cnt);
	atomic_sub(1, (atomic_t *)&spt_pg->bit_used);
	
	return;
}
#if 1

int get_grp_by_data(char *data, int pos)
{
	int start_bit = 0;
	unsigned int key_val = 0;
	unsigned int static_hash_key;
	int grp_static_len  = GRP_DYNAMIC_START - GRP_STATIC_START; 	
	int grp_dynamic_len  = GRP_SPILL_START - GRP_DYNAMIC_START; 	
	if (pos < 0 || pos > 4096)
		spt_debug("error pos");
	if (pos < GRP_DYNAMIC_POS) {
		start_bit = (pos>>2)<<2;		
		key_val = *(data + (start_bit >> 3));
		if ((start_bit % 8) == 0)
			key_val = key_val>>4;
		else
			key_val = key_val &0x0F;
		static_hash_key = (key_val << 16)+ start_bit;
		return jhash(&static_hash_key, 4, 16)%grp_static_len;
	}else
		return jhash(data + 2, 64, 13)*grp_dynamic_len + GRP_DYNAMIC_START;

}

int vec_alloc_spill_grp(struct cluster_head_t *pclst)
{
	int spill_grp_id;
	if ((pclst->spill_grp_id/GRPS_PER_PG) >= pclst->pg_num_max)
		return 0;
	
	spill_grp_id = atomic_add_return(1, (atomic_t*)&pclst->spill_grp_id);
	spill_grp_id--;
	return spill_grp_id;	
}

int vec_alloc_by_hash(struct cluster_head_t *pclst,  struct spt_vec **vec, char *data, int pos)
{
	struct spt_grp  *grp;
	int fs, gid, offset, gid_t;
	struct spt_pg_h *spt_pg;
	struct spt_grp old, tmp;

	gid = get_grp_by_data(data, pos);
re_alloc:
	spt_pg = get_vec_pg_head(pclst, gid/GRPS_PER_PG); /*get page head ,if page null alloc page*/
	grp  = get_grp_from_page_head(spt_pg, gid);
    while(1)
    {
		old = *grp;
		if((old.allocmap & GRP_ALLOCMAP_MASK) == 0)
		{
			gid = old.next_grp;
			if (gid == 0) {
				tmp = old;
				tmp.next_grp = 0xFFFFF;
				if(old.control == atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control)) {
					tmp.next_grp = vec_alloc_spill_grp(pclst);
					if (tmp.next_grp == 0) {
						spt_debug("spill grp overload\r\n");
						continue;
					}
					grp->next_grp = tmp.next_grp;
					gid = grp->next_grp;
				}
			}
			if (gid == 0xFFFFF)
				continue;
			goto re_alloc;
		}
		fs = find_next_bit(&old.val, 32, 0);
		if(fs >=32)
			goto re_alloc;
		tmp = old;
		tmp.allocmap = old.allocmap & (~(1 << fs));
		
        if(old.val == atomic64_cmpxchg((atomic64_t *)&grp->val, old.val, tmp.val))
            break;
    }
    atomic_sub(1, (atomic_t *)&pclst->free_vec_cnt);
    atomic_add(1, (atomic_t *)&pclst->used_vec_cnt);
	atomic_add(1, (atomic_t *)&spt_pg->bit_used);
	*vec = (struct spt_vec *)(grp + 8 + fs*sizeof(struct spt_vec));
	//printf("cluster %p,vec alloc id %d\r\n",pclst, gid*VEC_PER_GRP +fs);
	return gid*VEC_PER_GRP + fs;
}
#endif
