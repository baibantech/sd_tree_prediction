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
		pgrp = (page + GRP_SIZE*i);
		pgrp->val = 0;
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

u32 grp_alloc_map(struct cluster_head_t *pclst)
{
	int grps, grp_id;
	struct spt_grp *pgrp;
	u32 total=0;
	grps = pclst->pg_cursor*GRPS_PER_PG;	
	for(grp_id=0; grp_id < grps; grp_id++)
	{
		pgrp = (struct spt_grp *)grp_id_2_ptr(pclst, grp_id);
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
		bittotal = grp_alloc_map(pclst);
        spt_print("[cluster %d]",i);
		spt_print("%p [data_used]:%d [vec_used]:%d [bit_used]:%d \r\n", 
		pclst, pclst->data_total, pclst->used_vec_cnt, bittotal);
        i++;
    }
}

void grp_free_map(struct cluster_head_t *pclst)
{
	int grps, grp_id;
	struct spt_grp *pgrp, st_grp;
	u32 total=0;
	u64 va_old;
	u32 tick;
	
	grps = pclst->pg_cursor*GRPS_PER_PG;	
	for(grp_id=0; grp_id < grps; grp_id++)
	{
		pgrp = (struct spt_grp *)grp_id_2_ptr(pclst, grp_id);
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
		grp_free_map(pclst);
    }
	return;
}

char* vec_id_2_ptr(struct cluster_head_t * pclst,unsigned int id)
{
	return (char*)grp_id_2_ptr(pclst, id/VEC_PER_GRP) + 8 + id%VEC_PER_GRP*VBLK_SIZE;
}

char* db_id_2_ptr(struct cluster_head_t * pclst,unsigned int id)
{
	return (char*)grp_id_2_ptr(pclst, id/VEC_PER_GRP) + 8 + id%VEC_PER_GRP*VBLK_SIZE;
}



/**
 * get_pg_head - called by db_id_2_ptr & vec_id_2_ptr
 * @pclst: pointer of sd tree cluster head
 * @id: page id in this cluster
 * return the address of page head
 */
struct spt_pg_h  *get_pg_head(struct cluster_head_t *pclst, unsigned int id)
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
		page = (char *)pclst->pglist[id];
		if (page)
			goto ret_id;

	} else if ((id - direct_pgs) < indirect_pgs) {
		pg_id = id - direct_pgs;
		indir_page = (char **)pclst->pglist[CLST_IND_PG];
		if (indir_page != NULL) {
			offset = pg_id;
			page = indir_page[offset];
			if (page)
				goto ret_id;
		}
	} else if ((id - direct_pgs -  indirect_pgs) < double_pgs) {
		pg_id = id - direct_pgs -  indirect_pgs;
		dindir_page = (char ***)pclst->pglist[CLST_DIND_PG];
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
	cluster_add_page(pclst);
	goto get_page_id;
ret_id:
	return page + PG_HEAD_OFFSET;
}
char  *grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id)
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
		page = (char *)pclst->pglist[pg_id];
		while (page == 0) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			page = (char *)atomic64_read(
				(atomic64_t *)&pclst->pglist[pg_id]);
		}
	} else if ((pg_id - direct_pgs) < indirect_pgs) {
		pg_id -= direct_pgs;
		indir_page = (char **)pclst->pglist[CLST_IND_PG];
		while (indir_page == NULL) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			indir_page = (char **)atomic64_read(
				(atomic64_t *)&pclst->pglist[CLST_IND_PG]);
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
		dindir_page = (char ***)pclst->pglist[CLST_DIND_PG];
		while (dindir_page == NULL) {
			//printf("loop page id %d,line  %d\r\n", pg_id, __LINE__);
			smp_mb();/* ^^^ */
			dindir_page = (char ***)atomic64_read(
				(atomic64_t *)&pclst->pglist[CLST_DIND_PG]);
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

int cluster_add_page(struct cluster_head_t *pclst)	
{
	char *page, **indir_page, ***dindir_page;
	u32 old_head;
	int i, total, id, pg_id, offset, offset2;
	struct block_head_t *blk;
	int ptrs_bit = pclst->pg_ptr_bits;
	int ptrs = (1 << ptrs_bit);
	u64 direct_pgs = CLST_NDIR_PGS;
	u64 indirect_pgs = 1<<ptrs_bit;
	u64 double_pgs = 1<<(ptrs_bit*2);

	pg_id = atomic_add_return(1, (atomic_t *)&pclst->pg_cursor);
	pg_id--;
	if (pclst->pg_num_max <= pg_id) {
		atomic_sub(1, (atomic_t *)&pclst->pg_cursor);
		return CLT_FULL;
	}

	page = spt_alloc_zero_page();
	if (page == NULL)
		return CLT_NOMEM;

	grp_init_per_page(page);
	//printf("add page id %d,page ptr %p\r\n",pg_id, page);
	id = pg_id;
	if (id < direct_pgs) {
		pclst->pglist[id] = page;
		smp_mb();/* ^^^ */
	} else if ((id - direct_pgs) < indirect_pgs) {
		id -= direct_pgs;
		offset = id;
		if (offset == 0) {
			indir_page = (char **)spt_alloc_zero_page();

			if (indir_page == NULL)
				return CLT_NOMEM;
			pclst->pglist[CLST_IND_PG] = (char *)indir_page;
		} else {
			while (atomic64_read(
			(atomic64_t *)&pclst->pglist[CLST_IND_PG]) == 0)
				smp_mb();/* ^^^ */
		}
		indir_page = (char **)pclst->pglist[CLST_IND_PG];
		//printf("indir_page %p, offset %d\r\n", indir_page,offset);
		indir_page[offset] = page;
		smp_mb();/* ^^^ */
	} else if ((id - direct_pgs - indirect_pgs) < double_pgs) {
		id = id - direct_pgs - indirect_pgs;
		offset = id >> ptrs_bit;
		offset2 = id & (ptrs-1);
		if (id == 0) {
			dindir_page = (char ***)spt_alloc_zero_page();
			if (dindir_page == NULL)
				return CLT_NOMEM;
			pclst->pglist[CLST_DIND_PG] = (char *)dindir_page;
		} else {
			while (atomic64_read(
			(atomic64_t *)&pclst->pglist[CLST_DIND_PG]) == 0)
				smp_mb();/* ^^^ */

		}
		dindir_page = (char ***)pclst->pglist[CLST_DIND_PG];

		if (offset2 == 0) {
			indir_page = (char **)spt_alloc_zero_page();
			if (indir_page == NULL)
				return CLT_NOMEM;
			dindir_page[offset] = indir_page;
		} else {
			while (atomic64_read(
			(atomic64_t *)&dindir_page[offset]) == 0)
				smp_mb();/* ^^^ */
		}
		indir_page = dindir_page[offset];
		indir_page[offset2] = page;
		smp_mb();/* ^^^ */
	} else {
		spt_debug("warning: id is too big");
		return CLT_ERR;
	}
	return SPT_OK;
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

    vec = vec_alloc_from_grp(phead, 0, &pvec,__LINE__);
	if (pvec == 0) {
		spt_free(phead);
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
	char *grp;
	u64 va_old, va_new;
	int fs, gid, i;
	struct spt_pg_h *spt_pg;

	atomic64_add(1, (atomic64_t *)&g_spill_vec);
	pgid++;
	if(pgid >= pclst->pg_num_max) {
		*vec = NULL;
		return SPT_NULL;
	}
	while(1)
	{
		spt_pg = get_pg_head(pclst, pgid);
		if(spt_pg->bit_used < PG_SPILL_WATER_MARK)
		{
			gid = pgid*GRPS_PER_PG;
			for(i=0;i<GRPS_PER_PG;i++)
			{
				grp  = get_grp_from_page_head(spt_pg, gid);
				while(1)
				{
					va_old = *(u64 *)grp;
					if((va_old & GRP_ALLOCMAP_MASK) == 0)
					{
						break;
					}
					fs = find_next_bit(grp, 30, 0);
					if(fs >=30 )
					{
						break;
					}
					va_new = va_old & (~(1 << fs));
					
					if(va_old  == atomic64_cmpxchg((atomic64_t *)grp, va_old,va_new))
					{
						atomic_sub(1, (atomic_t *)&pclst->free_vec_cnt);
						atomic_add(1, (atomic_t *)&pclst->used_vec_cnt);
						atomic_add(1, (atomic_t *)&spt_pg->bit_used);
						*vec = (struct spt_vec *)(grp + 8 + fs*sizeof(struct spt_vec));
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
	char *grp;
	u64 va_old, va_new;
	int fs, ns, gid , i, retry;
	struct spt_pg_h *spt_pg;

	retry=0;
	pgid++;
	
	if(pclst->pg_num_max <= pgid) {
		*db = NULL;
		return SPT_NULL;
	}
	while(1)
	{
		spt_pg = get_pg_head(pclst, pgid);
		if(spt_pg->bit_used < PG_SPILL_WATER_MARK)
		{
			gid = pgid*GRPS_PER_PG;
			for(i=0;i<GRPS_PER_PG;i++)
			{
				grp  = get_grp_from_page_head(spt_pg, gid);
				while(1)
				{
					va_old = atomic64_read((atomic64_t *)grp);
					if((va_old & GRP_ALLOCMAP_MASK) == 0)
					{
						break;
					}
					fs = find_next_bit(grp, 30, 0);
					while(1) {
						if(fs >=29 )
							break;
						ns = find_next_bit(grp, 30, fs+1);
						if(ns == fs+1) {
							va_new = va_old & (~(3 << fs));
							
							if(va_old  == atomic64_cmpxchg((atomic64_t *)grp, va_old,va_new)) {
								atomic_add(1, (atomic_t *)&pclst->data_total);
								*db = (struct spt_dh *)(grp + 8 + fs*sizeof(struct spt_vec));
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
	char *grp;
	u64 va_old, va_new;
	int fs, gid, offset, gid_t;
	struct spt_pg_h *spt_pg;
	
	gid = gid_t = id/VEC_PER_GRP;
	offset = id%VEC_PER_GRP;
	//printf("alloc line %d\r\n",line);
re_alloc:
	if(( gid/GRPS_PER_PG) >= pclst->pg_num_max) {
		*vec = NULL;
		return SPT_NULL;
	}
	spt_pg = get_pg_head(pclst, gid/GRPS_PER_PG); /*get page head ,if page null alloc page*/
	grp  = get_grp_from_page_head(spt_pg, gid);
    while(1)
    {
        va_old = *(u64 *)grp;
		if((va_old & GRP_ALLOCMAP_MASK) == 0)
		{
			if(gid-gid_t >= GRPS_PER_PG)
			{
				return vec_alloc_from_spill(pclst, vec, gid/GRPS_PER_PG);
			}
			gid++;
			offset=0;
			goto re_alloc;
		}
		fs = find_next_bit(grp, 30, offset);
		if(fs >=30 )
		{
			if(gid-gid_t >= GRPS_PER_PG)
			{
				return vec_alloc_from_spill(pclst, vec, gid/GRPS_PER_PG);
			}
			gid++;
			offset=0;
			goto re_alloc;
		}
		va_new = va_old & (~(1 << fs));
		
        if(va_old  == atomic64_cmpxchg((atomic64_t *)grp, va_old,va_new))
            break;
    }
    atomic_sub(1, (atomic_t *)&pclst->free_vec_cnt);
    atomic_add(1, (atomic_t *)&pclst->used_vec_cnt);
	atomic_add(1, (atomic_t *)&spt_pg->bit_used);
	*vec = (struct spt_vec *)(grp + 8 + fs*sizeof(struct spt_vec));
	//printf("cluster %p,vec alloc id %d\r\n",pclst, gid*VEC_PER_GRP +fs);
	return gid*VEC_PER_GRP + fs;
}

int db_alloc_from_grp(struct cluster_head_t *pclst, int id, struct spt_dh **db)
{
	char *grp;
	u64 va_old, va_new;
	int fs, ns, gid, gid_t, offset;	
	struct spt_pg_h *spt_pg;
	
	gid = gid_t = id/VEC_PER_GRP;
	offset = id%VEC_PER_GRP;
	
re_alloc:
	if(( gid/GRPS_PER_PG) >= pclst->pg_num_max) {
		*db = NULL;
		return SPT_NULL;
	}
	spt_pg = get_pg_head(pclst, gid/GRPS_PER_PG);
	grp  = get_grp_from_page_head(spt_pg, gid);
	while(1)
	{
		va_old = atomic64_read((atomic64_t *)grp);
		if((va_old & GRP_ALLOCMAP_MASK) == 0)
		{
			if(gid-gid_t >= GRPS_PER_PG)
			{
				return db_alloc_from_spill(pclst, db, gid/GRPS_PER_PG);
			}
			gid++;
			offset=0;
			goto re_alloc;
		}
		fs = find_next_bit(grp, 30, offset);
		while(1)
		{
			if(fs >=29 )
			{
				if(gid-gid_t >= GRPS_PER_PG)
				{
					return db_alloc_from_spill(pclst, db, gid/GRPS_PER_PG);
				}
				gid++;
				offset=0;
				goto re_alloc;
			}
			ns = find_next_bit(grp, 30, fs+1);
			if(ns == fs+1)
				break;
			fs = ns;
		}
		va_new = va_old & (~(3 << fs));
		
		if(va_old  == atomic64_cmpxchg((atomic64_t *)grp, va_old,va_new))
			break;
	}
	atomic_add(1, (atomic_t *)&pclst->data_total);	
	atomic_add(2, (atomic_t *)&spt_pg->bit_used);
	*db = (struct spt_dh *)(grp + 8 + fs*sizeof(struct spt_vec));
    (*db)->rsv = 0;
    (*db)->pdata = NULL;
	//printf("cluster %p,db alloc id %d\r\n",pclst, gid*VEC_PER_GRP +fs);
	
	return gid*VEC_PER_GRP + fs;
}


void db_free(struct cluster_head_t *pclst, int id)
{
	char *grp;
	u64 va_old, va_new;
	u64 allocmap, freemap;
	int offset;
	u32 tick;
	struct spt_grp st_grp ;
	struct spt_pg_h *spt_pg;

	//printf("db free id %d\r\n",id);
	spt_pg = get_pg_head(pclst, id/VEC_PER_GRP/GRPS_PER_PG);
	grp  = get_grp_from_page_head(spt_pg, id/VEC_PER_GRP);
	offset = id%VEC_PER_GRP;

	while(1)
	{
		va_old = atomic64_read((atomic64_t *)grp);
		st_grp.val = va_old;
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
		st_grp.tick = tick;
		if(va_old  == atomic64_cmpxchg((atomic64_t *)grp, va_old, st_grp.val))
			break;		
	}
	atomic_sub(1, (atomic_t *)&pclst->data_total);
	atomic_sub(2, (atomic_t *)&spt_pg->bit_used);
	return;
}

void vec_free(struct cluster_head_t *pclst, int id)
{
	char *grp;
	u64 va_old, va_new;
	u64 allocmap, freemap;
	int offset;
	u32 tick;
	struct spt_grp st_grp ;
	struct spt_pg_h *spt_pg;
	//printf("vec free id %d\r\n",id);
	spt_pg = get_pg_head(pclst, id/VEC_PER_GRP/GRPS_PER_PG);
	grp  = get_grp_from_page_head(spt_pg, id/VEC_PER_GRP);
	offset = id%VEC_PER_GRP;

	while(1)
	{
		va_old = atomic64_read((atomic64_t *)grp);
		st_grp.val = va_old;
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
		st_grp.tick = tick;
		if(va_old  == atomic64_cmpxchg((atomic64_t *)grp, va_old, st_grp.val))
			break;		
	}
    atomic_add(1, (atomic_t *)&pclst->free_vec_cnt);
    atomic_sub(1, (atomic_t *)&pclst->used_vec_cnt);
	atomic_sub(1, (atomic_t *)&spt_pg->bit_used);
	
	return;
}
int test_add_page(struct cluster_head_t *pclst)
{
    char *page, **indir_page, ***dindir_page;
    u32 size;
    int pg_id, offset;
    int ptrs_bit = pclst->pg_ptr_bits;
    int ptrs = (1 << ptrs_bit);
    u64 direct_pgs = CLST_NDIR_PGS;
    u64 indirect_pgs = 1<<ptrs_bit;
    u64 double_pgs = 1<<(ptrs_bit*2);

    if(pclst->pg_num_max == pclst->pg_cursor)
    {
        return CLT_FULL;
    }

    page = spt_alloc_zero_page();
    if(page == NULL)
        return CLT_NOMEM;

    if(pclst->pg_cursor == pclst->pg_num_total)
    {
        struct cluster_head_t *new_head;
        size = (sizeof(char *)*pclst->pg_num_total + sizeof(struct cluster_head_t));
        if(size*2 >= PG_SIZE)
        {
            new_head = (struct cluster_head_t *)spt_alloc_zero_page();
            if(new_head == NULL)
            {
                spt_free_page(page);
                return CLT_NOMEM;
            }
            memcpy((char *)new_head, (char *)pclst, size);
            new_head->pg_num_total = pclst->pg_num_max;
        }
        else
        {
            new_head = spt_malloc(size*2);
            if(new_head == NULL)
            {
                spt_free_page(page);
                return CLT_NOMEM;
            }    
            memcpy(new_head, pclst, size);
            new_head->pg_num_total = (size*2-sizeof(struct cluster_head_t))/sizeof(char *);
        }
        spt_free(pclst);
        pclst = new_head;

        
    }
    pg_id = pclst->pg_cursor;
    if(pg_id < direct_pgs)
    {
        pclst->pglist[pg_id] = page;
    }
    else if((pg_id -= direct_pgs) < indirect_pgs)
    {
        indir_page = (char **)pclst->pglist[CLST_IND_PG];
        offset = pg_id;
        indir_page[offset] = page;
    }
    else if((pg_id -= indirect_pgs) < double_pgs)
    {
        dindir_page = (char ***)pclst->pglist[CLST_DIND_PG];
        offset = pg_id >> ptrs_bit;
        indir_page = dindir_page[offset];
        offset = pg_id & (ptrs-1);
        indir_page[offset] = page;
    }
    else
    {
        spt_debug("warning: id is too big");
        return 0;
    }
    pclst->pg_cursor++;
    return SPT_OK;
}


int test_add_N_page(struct cluster_head_t *pclst, int n)
{
    int i;
    for(i=0;i<n;i++)
    {
        if(SPT_OK != cluster_add_page(pclst))
        {
            spt_debug("%d\r\n", i);
            return -1;
        }        
    }
    return 0;
}

void test_vec_alloc_n_times(struct cluster_head_t *pclst)
{
    int i,vec_a;
    struct spt_vec *pvec_a, *pid_2_ptr;
    
    for(i=0; ; i++)
    {
        vec_a = vec_alloc_from_grp(pclst, 0,  &pvec_a,__LINE__);
        if(pvec_a == 0)
        {
            //printf("\r\n%d\t%s\ti:%d", __LINE__, __FUNCTION__, i);
            break;
        }
        pid_2_ptr = (struct spt_vec *)vec_id_2_ptr(pclst, vec_a);
        if(pid_2_ptr != pvec_a)
        {
            spt_debug("vec_a:%d pvec_a:%p pid_2_ptr:%p\r\n", vec_a,pvec_a,pid_2_ptr);
        }
    }
    spt_debug("total:%d\r\n", i);
    for(;i>0;i--)
    {
        vec_free(pclst, i-1);
    }
    spt_debug(" ==============done!\r\n");
    return;
}

