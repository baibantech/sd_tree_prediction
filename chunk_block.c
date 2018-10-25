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
		pgrp->allocmap = 0xFFFFull;
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
	return (char*)vec_grp_id_2_ptr(pclst, id/VEC_PER_GRP) + sizeof(struct spt_grp) + id%VEC_PER_GRP*VBLK_SIZE;
}

char* db_id_2_ptr(struct cluster_head_t * pclst,unsigned int id)
{
	return (char*)db_grp_id_2_ptr(pclst, id/VEC_PER_GRP) + sizeof(struct spt_grp)+ id%VEC_PER_GRP*VBLK_SIZE;
}

struct spt_pg_h  *get_pg_head(struct cluster_head_t *pclst, char **pglist, unsigned int id)
{
	int ptrs_bit = pclst->address_info.pg_ptr_bits;
	int ptrs = (1 << ptrs_bit);
	u64 direct_pgs = CLST_NDIR_PGS;
	u64 indirect_pgs = 1<<ptrs_bit;
	u64 double_pgs = 1<<(ptrs_bit*2);
	int pg_id = id ;
	int offset;
	char *page, **indir_page, ***dindir_page;
	page = NULL;	
	if (id < direct_pgs) {
		page = (char *)pglist[id];

	} else if ((id - direct_pgs) < indirect_pgs) {
		pg_id = id - direct_pgs;
		indir_page = (char **)pglist[CLST_IND_PG];
		if (indir_page != NULL) {
			offset = pg_id;
			page = indir_page[offset];
		}
	} else if ((id - direct_pgs -  indirect_pgs) < double_pgs) {
		pg_id = id - direct_pgs -  indirect_pgs;
		dindir_page = (char ***)pglist[CLST_DIND_PG];
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
	return (struct spt_pg_h *)(page + PG_HEAD_OFFSET);
}

struct spt_pg_h  *get_vec_pg_head(struct cluster_head_t *pclst, unsigned int id)
{
	char *page;
	do {
		page = get_pg_head(pclst, pclst->pglist_vec, id);
		if (page)
			return page;
		cluster_vec_add_page(pclst, id);
	}while(1);
}
struct spt_pg_h  *get_db_pg_head(struct cluster_head_t *pclst, unsigned int id)
{
	char *page;
	do {
		page = get_pg_head(pclst, pclst->pglist_db, id);
		if (page)
			return page;
		cluster_db_add_page(pclst, id);
	}while(1);
}

char *cluster_grp_id_2_ptr(struct cluster_head_t *pclst, char **pglist, unsigned int grp_id)
{
	int ptrs_bit = pclst->address_info.pg_ptr_bits;
	int ptrs = (1 << ptrs_bit);
	u64 direct_pgs = CLST_NDIR_PGS;
	u64 indirect_pgs = 1<<ptrs_bit;
	u64 double_pgs = 1<<(ptrs_bit*2);
	int pg_id = grp_id/GRPS_PER_PG;
	int offset;
	char *page, **indir_page, ***dindir_page;

	if (pg_id < direct_pgs) {
		page = (char *)pglist[pg_id];
		while (page == 0) {
			smp_mb();/* ^^^ */
			page = (char *)atomic64_read(
				(atomic64_t *)&pglist[pg_id]);
		}
	} else if ((pg_id - direct_pgs) < indirect_pgs) {
		pg_id -= direct_pgs;
		indir_page = (char **)pglist[CLST_IND_PG];
		while (indir_page == NULL) {
			smp_mb();/* ^^^ */
			indir_page = (char **)atomic64_read(
				(atomic64_t *)&pglist[CLST_IND_PG]);
		}
		offset = pg_id;
		page = indir_page[offset];
		while (page == 0) {
			smp_mb();/* ^^^ */
			page = (char *)atomic64_read(
				(atomic64_t *)&indir_page[offset]);
		}
	} else if ((pg_id - direct_pgs -  indirect_pgs) < double_pgs) {
		pg_id = pg_id - direct_pgs -  indirect_pgs;
		dindir_page = (char ***)pglist[CLST_DIND_PG];
		while (dindir_page == NULL) {
			smp_mb();/* ^^^ */
			dindir_page = (char ***)atomic64_read(
				(atomic64_t *)&pglist[CLST_DIND_PG]);
		}
		offset = pg_id >> ptrs_bit;
		indir_page = dindir_page[offset];
		while (indir_page == 0) {
			smp_mb();/* ^^^ */
			indir_page = (char **)atomic64_read(
				(atomic64_t *)&dindir_page[offset]);
		}
		offset = pg_id & (ptrs-1);
		page = indir_page[offset];
		while (page == 0) {
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

char *vec_grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id)
{
	return cluster_grp_id_2_ptr(pclst, pclst->pglist_vec, grp_id);
}

char *db_grp_id_2_ptr(struct cluster_head_t *pclst, unsigned int grp_id)
{
	return cluster_grp_id_2_ptr(pclst, pclst->pglist_db, grp_id);
}

void cluster_add_page(struct cluster_head_t *pclst, char **pglist, int pg_id)	
{
	char *page, **indir_page, ***dindir_page;
	u32 old_head;
	int i, total, id, offset, offset2;
	struct block_head_t *blk;
	int ptrs_bit = pclst->address_info.pg_ptr_bits;
	int ptrs = (1 << ptrs_bit);
	u64 direct_pgs = CLST_NDIR_PGS;
	u64 indirect_pgs = 1<<ptrs_bit;
	u64 double_pgs = 1<<(ptrs_bit*2);
	u64 old_pg;

	page = spt_alloc_zero_page();
	if (page == NULL)
		return;

	grp_init_per_page(page);
	id = pg_id;
	if (id < direct_pgs) {
		old_pg = pglist[id];
		if (old_pg == 0)
			old_pg = atomic64_cmpxchg((atomic64_t*)&pglist[id],
				   old_pg, (u64)page);
		if (old_pg)
			spt_free(page);
	} else if ((id - direct_pgs) < indirect_pgs) {
		id -= direct_pgs;
		offset = id;
		old_pg = pglist[CLST_IND_PG];
		if (old_pg == 0){
			indir_page = (char **)spt_alloc_zero_page();
			if (indir_page == NULL)
				return ;
			old_pg = atomic64_cmpxchg((atomic64_t*)&pglist[CLST_IND_PG],
					(u64)old_pg, (u64)indir_page);
		
			if (old_pg){
				spt_free(page);
				spt_free(indir_page);
				return;
			}
		}
		indir_page = (char **)pglist[CLST_IND_PG];
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

		old_pg = pglist[CLST_DIND_PG];
		if (old_pg == 0) {
			dindir_page = (char ***)spt_alloc_zero_page();
			if (dindir_page == NULL)
				return ;
			old_pg = atomic64_cmpxchg((atomic64_t*)&pglist[CLST_DIND_PG],
					(u64)old_pg, (u64)dindir_page);
			if (old_pg) {
				spt_free(dindir_page);
				spt_free(page);
				return;
			}
		}
		dindir_page = (char ***)pglist[CLST_DIND_PG];

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
void cluster_vec_add_page(struct cluster_head_t *pclst, int pg_id)	
{
	if (pclst->address_info.pg_num_max <= pg_id) {
		return;
	}
	cluster_add_page(pclst, pclst->pglist_vec, pg_id);
	atomic_add(1, (atomic_t *)&pclst->address_info.pg_vec_num_total);
	return;
}
void cluster_db_add_page(struct cluster_head_t *pclst, int pg_id)	
{
	if (pclst->address_info.pg_num_max <= pg_id) {
		return;
	}
	cluster_add_page(pclst, pclst->pglist_db, pg_id);	
	atomic_add(1, (atomic_t *)&pclst->address_info.pg_db_num_total);
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
	spt_vec_debug_info_init(phead);

    vec = vec_alloc(phead, &pvec, 0);
	if (pvec == 0) {
		spt_free(phead);
		spt_free(phead->pglist_db);
		return NULL;
	}
	phead->last_alloc_id = vec;
	pvec->val = 0;
	pvec->type = SPT_VEC_DATA;
	pvec->pos = startbit - 1;
	pvec->scan_status = SPT_VEC_PVALUE;
	pvec->down = SPT_NULL;
	pvec->rd = SPT_NULL;

	phead->vec_head = vec;
	phead->pstart = pvec;
	pg_num = (GRP_SPILL_START / GRPS_PER_PG) + 1; 
	for (i = 0 ; i < pg_num; i++) {
		//get_db_pg_head(phead, i);
		//get_vec_pg_head(phead, i);
	}

	return phead;
}

int db_alloc_from_spill(struct cluster_head_t *pclst, struct spt_dh **db, int pgid)
{
	struct spt_grp *grp;
	int fs, ns, gid , i, retry;
	struct spt_pg_h *spt_pg;
	struct spt_grp va_old, va_new;

	retry=0;
	pgid++;
	
	if(pclst->address_info.pg_num_max <= pgid) {
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
					
					fs = find_next_bit(&va_old.val, VEC_PER_GRP, 0);
					while(1) {
						if(fs >= VEC_PER_GRP - 1)
							break;
						
						ns = find_next_bit(&va_old.val, VEC_PER_GRP, fs+1);
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
		if(pclst->address_info.pg_num_max <= pgid)
		{
			*db = NULL;
			return SPT_NULL;
		}
	}
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
	if(( gid/GRPS_PER_PG) >= pclst->address_info.pg_num_max) {
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
		fs = find_next_bit(&va_old.val, VEC_PER_GRP, offset);
		while(1)
		{
			if(fs >= VEC_PER_GRP -1)
			{
				if(gid-gid_t >= GRPS_PER_PG)
				{
					return db_alloc_from_spill(pclst, db, gid/GRPS_PER_PG);
				}
				gid++;
				offset=0;
				goto re_alloc;
			}
			ns = find_next_bit(&va_old.val, VEC_PER_GRP, fs+1);
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

void vec_free(struct cluster_head_t *pclst, int vec_id)
{
	struct spt_grp *grp;
	struct spt_grp va_old;
	u64 allocmap, freemap;
	int offset;
	u32 tick;
	struct spt_grp st_grp ;
	struct spt_pg_h *spt_pg;
	//printf("vec free id %d\r\n",id);
	spt_pg = get_vec_pg_head(pclst, vec_id/VEC_PER_GRP/GRPS_PER_PG);
	grp  = get_grp_from_page_head(spt_pg, vec_id/VEC_PER_GRP);
	offset = vec_id%VEC_PER_GRP;

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
	atomic_sub(1, (atomic_t *)&spt_pg->bit_used);
	return;
}

int vec_alloc_spill_grp(struct cluster_head_t *pclst)
{
	int spill_grp_id = 0;
	if ((pclst->spill_grp_id/GRPS_PER_PG) >= pclst->address_info.pg_num_max)
		return 0;
	
	spill_grp_id = atomic_add_return(1, (atomic_t*)&pclst->spill_grp_id);
	spill_grp_id--;
	return spill_grp_id;	
}
int vec_alloc(struct cluster_head_t *pclst,  struct spt_vec **vec, unsigned int sed)
{
	struct spt_grp  *grp, *next_grp;
	int fs, gid, gid_t;
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
	gid = sed % GRP_SPILL_START ;


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
			fs = find_next_bit(&old.val, VEC_PER_GRP, vec_index);
			if(fs != vec_index) {
				old.control = grp->control;
				old.val = grp->val;
				tmp = old;
				alloc_by_index = 0;
				alloc_cnt++;
				goto alloc_next_grp;
			}
		} else {
			fs = find_next_bit(&old.val, VEC_PER_GRP, 0);
			if(fs >= VEC_PER_GRP)
				goto re_alloc;
		}
		tmp.allocmap = old.allocmap & (~(1 << fs));
		
        if(old.val == atomic64_cmpxchg((atomic64_t *)&grp->val, old.val, tmp.val))
            break;
    }
    atomic_add(1, (atomic_t *)&pclst->used_vec_cnt);
	atomic_add(1, (atomic_t *)&spt_pg->bit_used);
	*vec = (struct spt_vec *)((char *)grp + sizeof(struct spt_grp) + fs*sizeof(struct spt_vec));
	//printf("cluster %p,vec alloc id %d\r\n",pclst, gid*VEC_PER_GRP +fs);
	return gid*VEC_PER_GRP + fs;
}

