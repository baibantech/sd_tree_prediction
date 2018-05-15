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
extern int sd_perf_debug;
extern int sd_perf_debug_1;
int g_data_size;
unsigned int *pos_stat_ptr = NULL;

void pos_stat_mem_init(void)
{
	pos_stat_ptr = spt_malloc((g_data_size*8 + 1)*sizeof(unsigned int));
	if (pos_stat_ptr)
		memset(pos_stat_ptr,0, (g_data_size*8+1)*sizeof(unsigned int));
}
void pos_stat_inc(int pos)
{
	if(sd_perf_debug)
		pos_stat_ptr[pos]++;
}

void show_pos_stat(void)
{
	int i = 0;
	unsigned long long vec_total = 0;
	for (i = 0; i < g_data_size*8 ; i++)
	{
		if(pos_stat_ptr[i])
			printf("pos %d, vec cnt %d\r\n", i, pos_stat_ptr[i]);
		vec_total += pos_stat_ptr[i];
	}
	printf("vec total %d\r\n",vec_total);
}

void grp_init_per_page(char *page)
{
	int i;
	struct spt_grp *pgrp;
	for (i = 0 ; i < GRPS_PER_PG; i++) {
		pgrp = (page + GRP_SIZE*i);
		pgrp->val = 0;
		pgrp->control = 0;
		pgrp->allocmap = 0xFFFFFFFFull;
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
u32 vec_static_grp_alloc_map(struct cluster_head_t *pclst)
{
	int grps, grp_id;
	struct spt_grp *pgrp;
	u32 total=0;
	grps = GRP_DYNAMIC_START;	
	for(grp_id=0; grp_id < grps; grp_id++)
	{
		get_vec_pg_head(pclst, grp_id/GRPS_PER_PG); /*get page head ,if page null alloc page*/
		pgrp = (struct spt_grp *)vec_grp_id_2_ptr(pclst, grp_id);
		total += 32-calBitNum(pgrp->allocmap);
	}
	printf("alloc map total bit %d\r\n",total);
	return total;
}
#if 0
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

struct spt_pg_h  *get_vec_pg_head_no_page(struct cluster_head_t *pclst, unsigned int id)
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
	page = NULL;	
	if (id < direct_pgs) {
		page = (char *)pclst->pglist_vec[id];

	} else if ((id - direct_pgs) < indirect_pgs) {
		pg_id = id - direct_pgs;
		indir_page = (char **)pclst->pglist_vec[CLST_IND_PG];
		if (indir_page != NULL) {
			offset = pg_id;
			page = indir_page[offset];
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
			}
		}
	} else {
		spt_debug("warning: id is too big\r\n");
		return NULL;
	}
	if (!page)
		return NULL;
	return page + PG_HEAD_OFFSET;
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
	atomic_add(1, (atomic_t *)&pclst->pg_num_total);
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

void insert_jhash_value_stat(struct cluster_head_t *pclst, unsigned int hash_value)
{
	int i = 0;
	for (i = 0; i < pclst->jhash_value_cnt; i++) {
		if (pclst->jhash_value_stat[i] == hash_value)
			return;
	}

	if (pclst->jhash_value_cnt < 100000) {
		pclst->jhash_value_stat[i] = hash_value;
		pclst->jhash_value_cnt++;
		return;
	}
	printf("jhash valure too many \r\n");	
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

	phead->jhash_value_stat = spt_malloc(sizeof(unsigned int) *100000);
	phead->jhash_value_cnt = 0;
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
    atomic_add(1, (atomic_t *)&pclst->free_vec_cnt);
    atomic_sub(1, (atomic_t *)&pclst->used_vec_cnt);
	atomic_sub(1, (atomic_t *)&spt_pg->bit_used);
	
	return;
}
#if 1
int get_grp_by_data(struct cluster_head_t *pclst, char *data, int pos)
{
	int end_bit = 0;
	unsigned int key_val = 0;
	unsigned char key_byte;
	int i = 0;
	unsigned int static_hash_key;
	unsigned int hash_value;
	unsigned int grp_static_len  = GRP_DYNAMIC_START - GRP_STATIC_START; 	
	unsigned int grp_dynamic_len  = GRP_SPILL_START - GRP_DYNAMIC_START; 	
	if (pos < 0 || pos > 4096*8)
		spt_debug("error pos");
	end_bit = (pos>>2) << 2;
	if (end_bit <= GRP_DYNAMIC_POS) {
		if (end_bit == 0)
			return 0;
		for (i = 0; i < end_bit>>2; i++){
			key_byte = *(data+ (i>>1));
			if(i%2)
				key_byte = key_byte&0x0F;
			else
				key_byte = key_byte>>4;
			key_val = (key_val <<4) + key_byte;
		}
		static_hash_key = (key_val << 16)+ end_bit;
		if (sd_perf_debug_1)
			pclst->vec_stat[pos>>2]++;
		hash_value = (unsigned int)jhash(&static_hash_key, 4, 17);
		if (sd_perf_debug_1)
			insert_jhash_value_stat(pclst, static_hash_key);
		return hash_value%grp_static_len;
	}else {
		pclst->vec_stat[7]++;
		return ((unsigned int)jhash(data + 2, 64, 13)%grp_dynamic_len) + GRP_DYNAMIC_START;
	}
}
int get_vec_index_by_data(char *data, int pos)
{
	int len_byte = 0;
	unsigned int key_val = 0;
	unsigned char key_byte;
	int i = 0;
	unsigned int vec_index = 0;
	unsigned char key[5];
	int end_bit;
	if (pos < 0 || pos > 4096*8)
		spt_debug("error pos");
	len_byte = (pos + 1)/8;
	if (len_byte >= 5){
		printf("error len byte,pos %d\r\n",pos);
		sleep(1000);
	}
	if (len_byte == 0) {
		key_byte = *data;
		key_byte = key_byte >> (8 - pos - 1);
		key_val = key_byte;
	//	printf("key value is 0x%x\r\n",key_val);
		vec_index = (unsigned int)jhash(&key_val, 4, 17)%32;
	} else {
		end_bit = (pos + 1)%8;
		for (i=0 ; i < len_byte; i++) {
			key[i] = *(data + i);
	//		printf("key value is 0x%x\r\n",key[i]);
		}
		if (end_bit) {
			key_byte = *(data + i);
			key_byte = (key_byte >> (8 -end_bit)) << (8 - end_bit);
			key[i] = key_byte;
	//		printf("key value is 0x%x\r\n",key[i]);
		}
		vec_index = ((unsigned int)jhash(&key, i , 17) + pos)%32;
	}
	//printf("vec_index is %d\r\n", vec_index);

	return vec_index;
}


int get_grp_by_data_debug(struct cluster_head_t *pclst, char *data, int pos)
{
	int end_bit = 0;
	unsigned int key_val = 0;
	unsigned char key_byte;
	int i = 0;
	unsigned int ret;
	unsigned int static_hash_key;
	int grp_static_len  = GRP_DYNAMIC_START - GRP_STATIC_START; 	
	int grp_dynamic_len  = GRP_SPILL_START - GRP_DYNAMIC_START; 	
	if (pos < 0 || pos > 4096*8)
		spt_debug("error pos");
	printf("data %p, pos %d\r\n",data, pos);
	end_bit = (pos>>2) << 2;

	if (end_bit <= GRP_DYNAMIC_POS) {
		if (end_bit == 0)
			return 0;
		for (i = 0; i < end_bit>>2; i++){
			key_byte = *(data+ (i>>1));
			if(i%2)
				key_byte = key_byte&0x0F;
			else
				key_byte = key_byte>>4;
			printf("keybyte %x",key_byte);
			key_val = (key_val <<4) + key_byte;
		}
		printf("key_val 0x%x\r\n", key_val);
		static_hash_key = (key_val << 16)+ end_bit;
		printf("static_hash_key 0x%x\r\n", static_hash_key);
		ret =jhash(&static_hash_key, 4, 17);
		printf("jhash result 0x%x\r\n", ret);
		ret = ret%grp_static_len;
		printf("static len  0x%x\r\n", grp_static_len);
		printf("return result 0x%x\r\n", ret);
		return ret;
	}else {
		return (jhash(data + 2, 64, 13)%grp_dynamic_len) + GRP_DYNAMIC_START;
	}
}



int vec_alloc_spill_grp(struct cluster_head_t *pclst)
{
	int spill_grp_id = 0;
	if ((pclst->spill_grp_id/GRPS_PER_PG) >= pclst->pg_num_max)
		return 0;
	
	spill_grp_id = atomic_add_return(1, (atomic_t*)&pclst->spill_grp_id);
	spill_grp_id--;
	return spill_grp_id;	
}
int alloc_next_grp_max = 0;
int vec_alloc_by_hash(struct cluster_head_t *pclst,  struct spt_vec **vec, char *data, int pos)
{
	struct spt_grp  *grp, *next_grp;
	int fs, gid, offset, gid_t;
	struct spt_pg_h *spt_pg;
	struct spt_grp old, tmp;
	int cnt = 0;
	int spill_grp_id = 0;
	int alloc_debug = 0;
	int next_grp_id = 0;
	int vec_index = 0;
	int alloc_by_index = 0;
	u32 tick;
	int alloc_cnt = 0;
	gid = get_grp_by_data(pclst, data, pos);

	if (gid < GRP_DYNAMIC_START) {
		if (sd_perf_debug_1)
			atomic_add(1, (atomic_t *)&pclst->static_grp_alloc);
	}
	else
		atomic_add(1, (atomic_t *)&pclst->dynamic_grp_alloc);
	
	if (gid < GRP_DYNAMIC_START) {
		vec_index = get_vec_index_by_data(data, pos);
		alloc_by_index = 1;
	}
	pos_stat_inc(pos);
re_alloc:
	spt_pg = get_vec_pg_head(pclst, gid/GRPS_PER_PG); /*get page head ,if page null alloc page*/
	grp  = get_grp_from_page_head(spt_pg, gid);
		
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
			if (old.next_grp == 0) {
				tmp.next_grp = 0xFFFFF;
				if(old.control == atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control)) {
					next_grp_id = vec_alloc_spill_grp(pclst);
					next_grp  = get_grp_from_page_head(get_vec_pg_head(pclst, next_grp_id/GRPS_PER_PG), next_grp_id);
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
		if (alloc_by_index) {	
			if (sd_perf_debug_1)
				pclst->vec_static_alloc++;
			fs = find_next_bit(&old.val, 32, vec_index);
			if(fs != vec_index) {
				if (sd_perf_debug_1)
					pclst->vec_static_alloc_conflict++;
				old.control = grp->control;
				old.val = grp->val;
				tmp = old;
				alloc_by_index = 0;
				alloc_cnt++;
				goto alloc_next_grp;
			}
		} else {
			fs = find_next_bit(&old.val, 32, 0);
			if(fs >=32)
				goto re_alloc;
		}
		if (alloc_cnt > alloc_next_grp_max)
			alloc_next_grp_max = alloc_cnt;
		tmp.allocmap = old.allocmap & (~(1 << fs));
		
        if(old.val == atomic64_cmpxchg((atomic64_t *)&grp->val, old.val, tmp.val))
            break;
    }
    atomic_sub(1, (atomic_t *)&pclst->free_vec_cnt);
    atomic_add(1, (atomic_t *)&pclst->used_vec_cnt);
	atomic_add(1, (atomic_t *)&spt_pg->bit_used);
	*vec = (struct spt_vec *)((char *)grp + sizeof(struct spt_grp) + fs*sizeof(struct spt_vec));
	//printf("cluster %p,vec alloc id %d\r\n",pclst, gid*VEC_PER_GRP +fs);
	return gid*VEC_PER_GRP + fs;
}
int vec_alloc_by_hash_1(struct cluster_head_t *pclst,  struct spt_vec **vec, char *data, int pos)
{
	struct spt_grp  *grp, *next_grp;
	int fs, gid, offset, gid_t;
	struct spt_pg_h *spt_pg;
	struct spt_grp old, tmp;
	int cnt = 0;
	int spill_grp_id = 0;
	int alloc_debug = 0;
	int next_grp_id = 0;
	u32 tick;

	gid = get_grp_by_data(pclst, data, pos);

	if (gid < GRP_DYNAMIC_START)
		atomic_add(1, (atomic_t *)&pclst->static_grp_alloc);
	else
		atomic_add(1, (atomic_t *)&pclst->dynamic_grp_alloc);

re_alloc:
	spt_pg = get_vec_pg_head(pclst, gid/GRPS_PER_PG); /*get page head ,if page null alloc page*/
	grp  = get_grp_from_page_head(spt_pg, gid);
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
					}while (old.control  != atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control));
				continue;
				}
			}
		}
		if((old.allocmap & GRP_ALLOCMAP_MASK) == 0)
		{
			if (old.next_grp == 0) {
				tmp.next_grp = 0xFFFFF;
				if(old.control == atomic64_cmpxchg((atomic64_t *)&grp->control, old.control, tmp.control)) {
					next_grp_id = vec_alloc_spill_grp(pclst);
					next_grp  = get_grp_from_page_head(get_vec_pg_head(pclst, next_grp_id/GRPS_PER_PG), next_grp_id);
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
		fs = find_next_bit(&old.val, 32, 0);
		if(fs >=32)
			goto re_alloc;
		tmp.allocmap = old.allocmap & (~(1 << fs));
		
        if(old.val == atomic64_cmpxchg((atomic64_t *)&grp->val, old.val, tmp.val))
            break;
    }
    atomic_sub(1, (atomic_t *)&pclst->free_vec_cnt);
    atomic_add(1, (atomic_t *)&pclst->used_vec_cnt);
	atomic_add(1, (atomic_t *)&spt_pg->bit_used);
	*vec = (struct spt_vec *)((char *)grp + sizeof(struct spt_grp) + fs*sizeof(struct spt_vec));
	//printf("cluster %p,vec alloc id %d\r\n",pclst, gid*VEC_PER_GRP +fs);
	return gid*VEC_PER_GRP + fs;
}

int static_null_cnt;
int static_use_cnt;
int static_conflict_max= 0;
int static_max_grp;
int static_max_grp_array[10];
int static_vec_cnt_array[10];
void check_static_grp_stat(struct cluster_head_t *pclst)
{
	struct spt_grp *grp; 
	struct spt_pg_h *spt_pg;
	int i, grp_id;
	int cnt = 0;
	int vec_cnt = 0;
	static_null_cnt = 0;
	static_use_cnt = 0;
	static_conflict_max = 0;
	static_max_grp = 0;
	for (i = 0 ; i < 10 ; i++)
		static_max_grp_array[i] = 0;
	for (i = 0 ; i < 10 ; i++)
		static_vec_cnt_array[i] = 0;
	for (i = 0 ; i < GRP_DYNAMIC_START; i++){
		spt_pg = get_vec_pg_head_no_page(pclst, i/GRPS_PER_PG); /*get page head ,if page null alloc page*/
		if (!spt_pg) {
			static_null_cnt++;
			continue;
		}
		static_use_cnt++;
		grp  = get_grp_from_page_head(spt_pg, i);
		if (grp->allocmap == 0xFFFFFFFF) {
			static_null_cnt++;
			continue;
		}
		vec_cnt += 32 - calBitNum(grp->allocmap);
		while (grp){
			grp_id = grp->next_grp;
			if((grp_id !=0) && (grp_id != 0xFFFFF))
			{
			}else {
				break;
			}
			cnt++;
			spt_pg = get_vec_pg_head_no_page(pclst, grp_id/GRPS_PER_PG); /*get page head ,if page null alloc page*/
			grp  = get_grp_from_page_head(spt_pg, grp_id);
			vec_cnt += 32 - calBitNum(grp->allocmap);
		}
		if (cnt < 10) {
			static_max_grp_array[cnt]++;
			static_vec_cnt_array[cnt] += vec_cnt;
		} else
			printf("conflict too many\r\n");
		if (cnt > static_conflict_max){
			static_conflict_max = cnt;
			static_max_grp = i;
		}
		vec_cnt = 0;
		cnt =0;
	}

	printf("null cnt %d\r\n", static_null_cnt);
	printf("use cnt %d\r\n", static_use_cnt);
	printf("conflict max cnt %d\r\n", static_conflict_max);
	printf("max grp id %d\r\n", static_max_grp);
	for (i = 0 ; i < 10 ; i++)
		printf("conflict array cnt %d,%d,vec_cnt %d\r\n", i ,static_max_grp_array[i],static_vec_cnt_array[i]);

}

#endif
