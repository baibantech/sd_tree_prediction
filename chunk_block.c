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

void grp_init(struct cluster_head_t *pclst)
{
	int grps, grp_id;
	struct spt_grp *pgrp;
	grps = pclst->pg_num_max*GRPS_PER_PG;
	for(grp_id=0; grp_id < grps; grp_id++)
	{
		pgrp = (struct spt_grp *)grp_id_2_ptr(pclst, grp_id);
		pgrp->val = 0;
		pgrp->allocmap = 0x3FFFFFFFull;
	}
	return;
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
	
	grps = pclst->pg_num_max*GRPS_PER_PG;	
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
	
	grps = pclst->pg_num_max*GRPS_PER_PG;	
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



/**
 * blk_id_2_ptr - called by db_id_2_ptr & vec_id_2_ptr
 * @pclst: pointer of sd tree cluster head
 * @id: block id in this cluster
 * return the address of the block
 * a block consist of data block or vector
 */
char *blk_id_2_ptr111(struct cluster_head_t *pclst, unsigned int id)
{
	int ptrs_bit = pclst->pg_ptr_bits;
	int ptrs = (1 << ptrs_bit);
	u64 direct_pgs = CLST_NDIR_PGS;
	u64 indirect_pgs = 1<<ptrs_bit;
	u64 double_pgs = 1<<(ptrs_bit*2);
	int pg_id = id >> pclst->blk_per_pg_bits;
	int offset;
	char *page, **indir_page, ***dindir_page;

	if (pg_id < direct_pgs) {
		page = (char *)pclst->pglist[pg_id];
		while (page == 0) {
			smp_mb();/* ^^^ */
			page = (char *)atomic64_read(
				(atomic64_t *)&pclst->pglist[pg_id]);
		}
	} else if ((pg_id - direct_pgs) < indirect_pgs) {
		pg_id -= direct_pgs;
		indir_page = (char **)pclst->pglist[CLST_IND_PG];
		while (indir_page == NULL) {
			smp_mb();/* ^^^ */
			indir_page = (char **)atomic64_read(
				(atomic64_t *)&pclst->pglist[CLST_IND_PG]);
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
		dindir_page = (char ***)pclst->pglist[CLST_DIND_PG];
		while (dindir_page == NULL) {
			smp_mb();/* ^^^ */
			dindir_page = (char ***)atomic64_read(
				(atomic64_t *)&pclst->pglist[CLST_DIND_PG]);
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

	offset = id & (pclst->blk_per_pg-1);
	return page + (offset << BLK_BITS);

}
#if 0
/* data block ptr = block ptr + db offset in the block */
char *db_id_2_ptr(struct cluster_head_t *pclst, unsigned int id)
{
	return blk_id_2_ptr(pclst, id/pclst->db_per_blk)
		+ id%pclst->db_per_blk * DBLK_SIZE;
}
/* vector ptr = block ptr + vector offset in the block */
char *vec_id_2_ptr(struct cluster_head_t *pclst, unsigned int id)
{
	return blk_id_2_ptr(pclst, id/pclst->vec_per_blk)
		+ id%pclst->vec_per_blk * VBLK_SIZE;
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
/**
 * cluster_add_page - cluster alloc one page ,and cut it into block
 * @pclst: pointer of sd tree cluster head
 *
 *a cluster use a page as the smallest unit to alloc for memory
 * a cluster maximum memory overhead is 64M
 */
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
	if (pclst->pg_num_max <= pg_id) {
		atomic_sub(1, (atomic_t *)&pclst->pg_cursor);
		return CLT_FULL;
	}

    pg_id--;
	page = (char *)pclst->base_addr + pg_id*4096;
	memset(page, 0, PG_SIZE);
   // id = pg_id;

    
    id = (pg_id<< pclst->blk_per_pg_bits);
//    pclst->pg_cursor++;
	total = pclst->blk_per_pg;
	blk = (struct block_head_t *)page;

	for (i = 1; i < total; i++) {
		blk->magic = 0xdeadbeef;
		blk->next = id + i;
		blk = (struct block_head_t *)(page + (i << BLK_BITS));
	}
	blk->magic = 0xdeadbeef;
	do {
		old_head = atomic_read((atomic_t *)&pclst->blk_free_head);
		blk->next = old_head;
//        pclst->blk_free_head = id;
	} while (old_head != atomic_cmpxchg(
	(atomic_t *)&pclst->blk_free_head, old_head, id));

	atomic_add(total, (atomic_t *)&pclst->free_blk_cnt);
	return SPT_OK;
}

void cluster_destroy(struct cluster_head_t *pclst)
{
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
	phead->blk_per_pg_bits = PG_BITS - BLK_BITS;
	phead->pg_ptr_bits = PG_BITS - ptr_bits;
	phead->blk_per_pg = PG_SIZE/BLK_SIZE;
	phead->db_per_blk = BLK_SIZE/DBLK_SIZE;
	phead->vec_per_blk = BLK_SIZE/VBLK_SIZE;
    phead->db_per_page= 4096/DBLK_SIZE; 
    phead->vec_per_page = 4096/VBLK_SIZE;
	phead->vec_free_head = -1;
	phead->blk_free_head = -1;
	phead->dblk_free_head = -1;

	phead->debug = 1;
	phead->is_bottom = is_bottom;
	phead->startbit = startbit;
	phead->endbit = endbit;
	phead->thrd_total = thread_num;
	phead->freedata = pf_free;
	phead->construct_data = pf_con;
	phead->base_addr = malloc(CLST_PG_NUM_MAX*4096); 
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

	phead->thrd_data = (struct spt_thrd_data *)spt_malloc(
		sizeof(struct spt_thrd_data)*thread_num);
	if (phead->thrd_data == NULL) {
		cluster_destroy(phead);
		return NULL;
	}
	for (i = 0; i < thread_num; i++) {
		phead->thrd_data[i].thrd_id = i;
		phead->thrd_data[i].vec_cnt = 0;
		phead->thrd_data[i].vec_list_cnt = 0;
		phead->thrd_data[i].data_cnt = 0;
		phead->thrd_data[i].data_list_cnt = 0;
		phead->thrd_data[i].vec_free_in = SPT_NULL;
		phead->thrd_data[i].vec_alloc_out = SPT_NULL;
		phead->thrd_data[i].data_free_in = SPT_NULL;
		phead->thrd_data[i].data_alloc_out = SPT_NULL;
		phead->thrd_data[i].rsv_cnt = 0;
		phead->thrd_data[i].rsv_list = SPT_NULL;
        //fill_in_rsv_list_simple(phead, SPT_PER_THRD_RSV_CNT, i);
	}
    //test_add_N_page(phead, phead->pg_num_max);
	grp_init(phead);
    vec = vec_alloc_from_grp(phead, 0, &pvec);
	if (pvec == 0) {
		cluster_destroy(phead);
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
/**
 * blk_alloc - alloc block
 * @pcluster: pointer of sd tree cluster head
 * @blk: return value, store the addr of the block
 *
 * return block id
 */
int blk_alloc(struct cluster_head_t *pcluster, char **blk)
{
	u32 blk_id, new_head;
	struct block_head_t *pblk;
	int try_cnts = 0;

	do {
		while ((blk_id = atomic_read(
			(atomic_t *)&pcluster->blk_free_head)) == -1) {
			if (cluster_add_page(pcluster) != SPT_OK) {
				if (try_cnts != 0) {
					*blk = NULL;
					return -1;
				}
				try_cnts++;
				continue;
			}
		}
		pblk = (struct block_head_t *)blk_id_2_ptr(pcluster, blk_id);
		new_head = pblk->next;
	} while (blk_id != atomic_cmpxchg(
	(atomic_t *)&pcluster->blk_free_head, blk_id, new_head));

	atomic_sub(1, (atomic_t *)&pcluster->free_blk_cnt);
	*blk = (char *)pblk;

	return blk_id;
}
/**
 * db_add_blk - alloc a block and cut it into data block
 * @pclst: pointer of sd tree cluster head
 */
int db_add_blk(struct cluster_head_t *pclst)
{
	char *pblk;
	int i, total;
	u32 id, blkid, old_head;
	struct db_head_t *db;

	blkid = blk_alloc(pclst, &pblk);
	if (pblk == NULL)
		return CLT_NOMEM;

	total = pclst->db_per_blk;
	id = (blkid*total);
	db = (struct db_head_t *)pblk;

	for (i = 1; i < total; i++) {
		db->magic = 0xdeadbeef;
		db->next = id + i;
		db = (struct db_head_t *)(pblk + (i * DBLK_SIZE));
	}
	db->magic = 0xdeadbeef;

	do {
		old_head = atomic_read((atomic_t *)&pclst->dblk_free_head);
		db->next = old_head;
	} while (old_head != atomic_cmpxchg(
	(atomic_t *)&pclst->dblk_free_head, old_head, id));

	atomic_add(total, (atomic_t *)&pclst->free_dblk_cnt);

	return SPT_OK;
}
/**
 * vec_add_blk - alloc a block and cut it into vectors
 * @pclst: pointer of sd tree cluster head
 */
int vec_add_blk(struct cluster_head_t *pclst)
{
	char *pblk;
	int i, total;
	u32 id, blkid, old_head;
	struct vec_head_t *vec;

	blkid = blk_alloc(pclst, &pblk);
	if (pblk == NULL)
		return CLT_NOMEM;

	total = pclst->vec_per_blk;
	id = (blkid*total);
	vec = (struct vec_head_t *)pblk;

	for (i = 1; i < total; i++) {
		vec->magic = 0xdeadbeef;
		vec->next = id + i;
		vec = (struct vec_head_t *)(pblk + (i << VBLK_BITS));
	}

	vec->magic = 0xdeadbeef;

	do {
		old_head = atomic_read((atomic_t *)&pclst->vec_free_head);
		vec->next = old_head;
		smp_mb();/* ^^^ */
	} while (old_head != atomic_cmpxchg(
	(atomic_t *)&pclst->vec_free_head, old_head, id));

	atomic_add(total, (atomic_t *)&pclst->free_vec_cnt);

	return SPT_OK;
}

/**
 * db_alloc - alloc data block from freelist
 * @pclst: pointer of sd tree cluster head
 * @db: return value, store the addr of the data block
 *
 * return data block id
 */
unsigned int db_alloc(struct cluster_head_t *pclst, struct spt_dh **db)
{
	u32 db_id;
	struct db_head_t *pdb;
	u32 try_cnts, new_head;

	try_cnts = 0;

	do {
		while ((db_id = atomic_read(
		(atomic_t *)&pclst->dblk_free_head)) == -1) {
			if (db_add_blk(pclst) != SPT_OK) {
				if (try_cnts != 0) {
					*db = NULL;
					return -1;
				}
				try_cnts++;
				continue;
			}
		}
		pdb = (struct db_head_t *)db_id_2_ptr(pclst, db_id);
		smp_mb();/* ^^^ */
		new_head = pdb->next;
	} while (db_id != atomic_cmpxchg(
	(atomic_t *)&pclst->dblk_free_head, db_id, new_head));

	atomic_sub(1, (atomic_t *)&pclst->free_dblk_cnt);
	atomic_add(1, (atomic_t *)&pclst->used_dblk_cnt);
	*db = (struct spt_dh *)pdb;
	(*db)->rsv = 0;
	(*db)->pdata = NULL;
	return db_id;
}
#if 0
/**
 * db_free - free data block to freelist
 * @pcluster: pointer of sd tree cluster head
 * @id: data block id
 */
void db_free(struct cluster_head_t *pcluster, int id)
{
	struct db_head_t *pdb;
	u32 old_head;

	pdb = (struct db_head_t *)db_id_2_ptr(pcluster, id);
	do {
		old_head = atomic_read(
			(atomic_t *)&pcluster->dblk_free_head);
		pdb->next = old_head;
		smp_mb();/* ^^^ */
	} while (old_head != atomic_cmpxchg(
	(atomic_t *)&pcluster->dblk_free_head, old_head, id));

	atomic_add(1, (atomic_t *)&pcluster->free_dblk_cnt);
	atomic_sub(1, (atomic_t *)&pcluster->used_dblk_cnt);
}
#endif
/**
 * vec_alloc - alloc vector from freelist
 * @pclst: pointer of sd tree cluster head
 * @vec: return value, store the addr of the vector
 *
 * return vector id
 */
unsigned int vec_alloc(struct cluster_head_t *pclst, struct spt_vec **vec)
{
	u32 vec_id;
	struct vec_head_t *pvec;
	u32 try_cnts, new_head;

	do {
		while ((vec_id = atomic_read(
		(atomic_t *)&pclst->vec_free_head)) == -1) {
			if (vec_add_blk(pclst) != SPT_OK) {
				try_cnts++;
				if (try_cnts != 0) {
					*vec = NULL;
					return -1;
				}
			}
		}
		pvec = (struct vec_head_t *)vec_id_2_ptr(pclst, vec_id);
		smp_mb();/* ^^^ */
		new_head = pvec->next;
	} while (vec_id != atomic_cmpxchg(
	(atomic_t *)&pclst->vec_free_head, vec_id, new_head));

	atomic_sub(1, (atomic_t *)&pclst->free_vec_cnt);
	atomic_add(1, (atomic_t *)&pclst->used_vec_cnt);
	*vec = (struct spt_vec *)pvec;

	return vec_id;
}
#if 0
/**
 * vec_free - free data block to freelist
 * @pcluster: pointer of sd tree cluster head
 * @id: vector id
 */
void vec_free(struct cluster_head_t *pcluster, int id)
{
	struct vec_head_t *pvec;
	u32 old_head;

	pvec = (struct vec_head_t *)vec_id_2_ptr(pcluster, id);
	do {
		old_head = atomic_read((atomic_t *)&pcluster->vec_free_head);
		pvec->next = old_head;
		smp_mb();/* ^^^ */
	} while (old_head != atomic_cmpxchg(
	(atomic_t *)&pcluster->vec_free_head, old_head, id));

	atomic_add(1, (atomic_t *)&pcluster->free_vec_cnt);
	atomic_sub(1, (atomic_t *)&pcluster->used_vec_cnt);
}
#endif
u64 g_spill_vec = 0;
int vec_alloc_from_spill(struct cluster_head_t *pclst, struct spt_vec **vec)
{
	char *grp;
	u64 va_old, va_new;
	int fs, gid, pgid, i;
	struct spt_pg_h *spt_pg;

	atomic64_add(1, (atomic64_t *)&g_spill_vec);

	pgid = pclst->pg_cursor;
	while(1)
	{
		spt_pg = get_pg_head(pclst, pgid);
		if(spt_pg->bit_used < PG_SPILL_WATER_MARK)
		{
			gid = pgid*GRPS_PER_PG;
			for(i=0;i<GRPS_PER_PG;i++)
			{
				grp  = grp_id_2_ptr(pclst, gid);
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
						return gid*VEC_PER_GRP + fs;
					}
				}
				gid++;
			}	
		}
		pgid = atomic_add_return(1, (atomic_t *)&pclst->pg_cursor);
		if(pclst->pg_num_max <= pgid)
		{
			atomic_sub(1, (atomic_t *)&pclst->pg_cursor);
			*vec = NULL;
			return SPT_NULL;
		}
	}
}


int db_alloc_from_spill(struct cluster_head_t *pclst, struct spt_dh **db)
{
	char *grp;
	u64 va_old, va_new;
	int fs, ns, gid, pgid, i, retry;
	struct spt_pg_h *spt_pg;

	retry=0;
	pgid = pclst->pg_cursor;
	while(1)
	{
		spt_pg = get_pg_head(pclst, pgid);
		if(spt_pg->bit_used < PG_SPILL_WATER_MARK)
		{
			gid = pgid*GRPS_PER_PG;
			for(i=0;i<GRPS_PER_PG;i++)
			{
				grp  = grp_id_2_ptr(pclst, gid);
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
		pgid = atomic_add_return(1, (atomic_t *)&pclst->pg_cursor);
		if(pclst->pg_num_max <= pgid)
		{
			atomic_sub(1, (atomic_t *)&pclst->pg_cursor);
			*db = NULL;
			return SPT_NULL;
		}
	}
}
int vec_alloc_from_grp(struct cluster_head_t *pclst, int id, struct spt_vec **vec)
{
	char *grp;
	u64 va_old, va_new;
	int fs, gid, offset, gid_t;
	struct spt_pg_h *spt_pg;
	
	gid = gid_t = id/VEC_PER_GRP;
	offset = id%VEC_PER_GRP;
		
re_alloc:
	spt_pg = get_pg_head(pclst, gid/GRPS_PER_PG);
	grp  = grp_id_2_ptr(pclst, gid);
    while(1)
    {
        va_old = *(u64 *)grp;
		if((va_old & GRP_ALLOCMAP_MASK) == 0)
		{
			if(gid-gid_t >= 1)//GRPS_PER_PG)
			{
				return vec_alloc_from_spill(pclst, vec);
			}
			gid++;
			offset=0;
			goto re_alloc;
		}
		fs = find_next_bit(grp, 30, offset);
		if(fs >=30 )
		{
			if(gid-gid_t >= 1)//GRPS_PER_PG)
			{
				return vec_alloc_from_spill(pclst, vec);
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
	spt_pg = get_pg_head(pclst, gid/GRPS_PER_PG);
	grp  = grp_id_2_ptr(pclst, gid);
	while(1)
	{
		va_old = atomic64_read((atomic64_t *)grp);
		if((va_old & GRP_ALLOCMAP_MASK) == 0)
		{
			if(gid-gid_t >= 1)
			{
				return db_alloc_from_spill(pclst, db);
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
				if(gid-gid_t >= 1)
				{
					return db_alloc_from_spill(pclst, db);
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

	grp  = grp_id_2_ptr(pclst, id/VEC_PER_GRP);
	offset = id%VEC_PER_GRP;
	spt_pg = get_pg_head(pclst, id/VEC_PER_GRP/GRPS_PER_PG);

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

	grp  = grp_id_2_ptr(pclst, id/VEC_PER_GRP);
	offset = id%VEC_PER_GRP;
	spt_pg = get_pg_head(pclst, id/VEC_PER_GRP/GRPS_PER_PG);

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

/**
 * db_alloc_from_buf - alloc data block from thread buffer list
 * @pclst: pointer of sd tree cluster head
 * @thread_id: thread id
 * @db: return value, store the addr of the data block
 *
 * return data block id
 */
unsigned int db_alloc_from_buf(struct cluster_head_t *pclst,
	int thread_id,
	struct spt_dh **db)
{
	struct spt_thrd_data *pthrd_data = &pclst->thrd_data[thread_id];
	u32 list_vec_id, ret_id;
	unsigned int tick;
	struct spt_buf_list *pnode;

	pnode = 0;
	list_vec_id = pthrd_data->data_alloc_out;

	if (list_vec_id == SPT_NULL) {
		*db = NULL;
		return -1;
	}
	pnode = (struct spt_buf_list *)vec_id_2_ptr(pclst, list_vec_id);
	tick = atomic_add_return(0, (atomic_t *)&g_thrd_h->tick)
		& SPT_BUF_TICK_MASK;
	if (tick <  pnode->tick)
		tick = tick | (1<<SPT_BUF_TICK_BITS);
	if (tick-pnode->tick < 2) {
		*db = NULL;
		return -1;
	}
	ret_id = pnode->id;
	*db = (struct spt_dh *)db_id_2_ptr(pclst, ret_id);
	if ((*db)->pdata != NULL) {
		if (spt_data_free_flag(*db))
			pclst->freedata((*db)->pdata);
	}
	(*db)->rsv = 0;
	pnode->id = SPT_NULL;
	if (pthrd_data->data_alloc_out
		== pthrd_data->data_free_in) {
		pthrd_data->data_alloc_out
		= pthrd_data->data_free_in
		= SPT_NULL;
	} else {
		pthrd_data->data_alloc_out = pnode->next;
	}
	pthrd_data->data_cnt--;
	pthrd_data->data_list_cnt--;
	vec_free(pclst, list_vec_id);
	pthrd_data->vec_cnt--;
	return ret_id;
}
/**
 * vec_alloc_from_buf - alloc vector from thread buffer list
 * @pclst: pointer of sd tree cluster head
 * @thread_id: thread id
 * @vec: return value, store the addr of the vector
 *
 * return vector id
 */
unsigned int vec_alloc_from_buf(struct cluster_head_t *pclst,
						int thread_id,
						struct spt_vec **vec)
{
	struct spt_thrd_data *pthrd_data = &pclst->thrd_data[thread_id];
	u32 list_vec_id, ret_id;
	u32 tick;
	struct spt_buf_list *pnode;

	pnode = 0;
	list_vec_id = pthrd_data->vec_alloc_out;

	if (list_vec_id == SPT_NULL) {
		*vec = NULL;
		return -1;
	}
	pnode = (struct spt_buf_list *)vec_id_2_ptr(
		pclst, list_vec_id);
	if (pnode->id == SPT_NULL) {
		if (pthrd_data->vec_alloc_out
			== pthrd_data->vec_free_in) {
			pthrd_data->vec_alloc_out
				= pthrd_data->vec_free_in
				= SPT_NULL;
		} else {
			pthrd_data->vec_alloc_out = pnode->next;
		}
		ret_id = list_vec_id;
		*vec = (struct spt_vec *)pnode;
		pthrd_data->vec_cnt--;
		pthrd_data->vec_list_cnt--;
		return ret_id;
	}

	tick = atomic_add_return(0,
		(atomic_t *)&g_thrd_h->tick) & SPT_BUF_TICK_MASK;
	if (tick <  pnode->tick)
		tick = tick | (1<<SPT_BUF_TICK_BITS);
	if (tick-pnode->tick < 2) {
		*vec = NULL;
		return -1;
	}
	ret_id = pnode->id;
	*vec = (struct spt_vec *)vec_id_2_ptr(pclst, ret_id);
	pnode->id = SPT_NULL;
	pthrd_data->vec_cnt--;
	return ret_id;
}
/**
 * in some scenes ,vector alloc must succeed.
 * every thread reserve several vector for
 * data block and vector recycling.
 */
int rsv_list_fill_cnt(struct cluster_head_t *pclst, int thread_id)
{
	return SPT_PER_THRD_RSV_CNT - pclst->thrd_data[thread_id].rsv_cnt;
}
/**
 * alloc can not fail
 */
unsigned int vec_alloc_from_rsvlist(struct cluster_head_t *pclst,
	int thread_id,
	struct spt_vec **vec)
{
	struct spt_thrd_data *pthrd_data = &pclst->thrd_data[thread_id];
	u32 vec_id;
	struct vec_head_t *pvec;

	vec_id = pthrd_data->rsv_list;
	pvec = (struct vec_head_t *)vec_id_2_ptr(pclst, vec_id);

	pthrd_data->rsv_list = pvec->next;
	pthrd_data->rsv_cnt--;
	*vec = (struct spt_vec *)pvec;
	return vec_id;
}
/* add vector into reserved list
 * if buffer list exceed water mark, free the vector&db that can be freed
 */
int fill_in_rsv_list(struct cluster_head_t *pclst, int nr, int thread_id)
{
	struct spt_thrd_data *pthrd_data = &pclst->thrd_data[thread_id];

	if (pthrd_data->vec_list_cnt
		> SPT_BUF_VEC_WATERMARK) {
		vec_buf_free(pclst, thread_id);
	}
	if (pthrd_data->data_list_cnt
		> SPT_BUF_DATA_WATERMARK) {
		db_buf_free(pclst, thread_id);
	}
/* if buffer list still exceed water mark, return SPT_WAIT_AMT
 * when tick refresh twice, the buffer list can be freed
 */
	if (atomic_read((atomic_t *)&pthrd_data->vec_list_cnt)
		> SPT_BUF_VEC_WATERMARK
		|| (atomic_read((atomic_t *)&pthrd_data->data_list_cnt)
		> SPT_BUF_DATA_WATERMARK)) {
		fill_in_rsv_list_simple(pclst, nr, thread_id);
		pclst->status = SPT_WAIT_AMT;
		return SPT_WAIT_AMT;
	} else {
		return fill_in_rsv_list_simple(pclst, nr, thread_id);
	}
}

/* add vector into reserved list */
int fill_in_rsv_list_simple(struct cluster_head_t *pclst, int nr, int thread_id)
{
	struct spt_thrd_data *pthrd_data = &pclst->thrd_data[thread_id];
	u32 vec_id;
	struct vec_head_t *pvec;

	while (nr > 0) {
		vec_id = vec_alloc_from_buf(
			pclst, thread_id, (struct spt_vec **)&pvec);
		if (pvec == NULL) {
			vec_id = vec_alloc(pclst, (struct spt_vec **)&pvec);
			if (pvec == NULL)
				return SPT_NOMEM;
		}
		pvec->next = pthrd_data->rsv_list;
		pthrd_data->rsv_list = vec_id;
		pthrd_data->rsv_cnt++;
		nr--;
	}
	return SPT_OK;
}
/**
 * free the vector in the buffer list into free list
 * the vector can be freed only if it is freed to buffer list 2 tick ago
 */
void vec_buf_free(struct cluster_head_t *pclst, int thread_id)
{
	struct spt_thrd_data *pthrd_data = &pclst->thrd_data[thread_id];
	u32 list_vec_id, tmp_id;
	u32 tick;
	struct spt_buf_list *pnode;

	tick = atomic_add_return(0, (atomic_t *)&g_thrd_h->tick)
		& SPT_BUF_TICK_MASK;
	list_vec_id = pthrd_data->vec_alloc_out;

	while (list_vec_id != pthrd_data->vec_free_in) {
		pnode = (struct spt_buf_list *)vec_id_2_ptr(
			pclst, list_vec_id);
		if (tick <  pnode->tick)
			tick = tick | (1<<SPT_BUF_TICK_BITS);
		if (tick - pnode->tick < 2) {
			pthrd_data->vec_alloc_out = list_vec_id;
			return;
		}
		if (pnode->id != SPT_NULL) {
			vec_free(pclst, pnode->id);
			pthrd_data->vec_cnt--;
		}
		tmp_id = list_vec_id;
		list_vec_id = pnode->next;
		vec_free(pclst, tmp_id);
		pthrd_data->vec_list_cnt--;
		pthrd_data->vec_cnt--;
	}
	pthrd_data->vec_alloc_out = list_vec_id;
}
/* free vector into buffer list
 * if buffer list exceed water mark, free the vector that can be freed
 * supplement vector into reserved list
 */
int vec_free_to_buf(struct cluster_head_t *pclst, int id, int thread_id)
{
	struct spt_thrd_data *pthrd_data = &pclst->thrd_data[thread_id];
	u32 list_vec_id;
	u32 tick;
	struct spt_buf_list *pnode;
	struct spt_vec *pvec;

	pvec = (struct spt_vec *)vec_id_2_ptr(pclst, id);
	pvec->status = SPT_VEC_RAW;
	list_vec_id = vec_alloc_from_rsvlist(
		pclst, thread_id, (struct spt_vec **)&pnode);
	tick = atomic_add_return(0, (atomic_t *)&g_thrd_h->tick)
		& SPT_BUF_TICK_MASK;
	pnode->tick = tick;
	pnode->id = id;
	pnode->next = SPT_NULL;
	if (pthrd_data->vec_free_in == SPT_NULL) {
		pthrd_data->vec_free_in
			= pthrd_data->vec_alloc_out
			= list_vec_id;
	} else {
		pnode = (struct spt_buf_list *)vec_id_2_ptr(
			pclst, pthrd_data->vec_free_in);
		pnode->next = list_vec_id;
		pthrd_data->vec_free_in = list_vec_id;
	}
	pthrd_data->vec_list_cnt++;
	pthrd_data->vec_cnt += 2;

	if (pthrd_data->vec_list_cnt > SPT_BUF_VEC_WATERMARK)
		vec_buf_free(pclst, thread_id);
	if (atomic_read((atomic_t *)&pthrd_data->vec_list_cnt)
		> SPT_BUF_VEC_WATERMARK) {
		fill_in_rsv_list_simple(pclst, 1, thread_id);
		pclst->status = SPT_WAIT_AMT;
		return SPT_WAIT_AMT;
	} else
		return fill_in_rsv_list_simple(pclst, 1, thread_id);
}
/**
 * free the db in the buffer list into free list
 * the db can be freed only if it is freed to buffer list 2 tick ago
 */
void db_buf_free(struct cluster_head_t *pclst, int thread_id)
{
	struct spt_thrd_data *pthrd_data = &pclst->thrd_data[thread_id];
	u32 list_vec_id, tmp_id;
	u32 tick;
	struct spt_buf_list *pnode;
	struct spt_dh *pdh;

	tick = atomic_add_return(0, (atomic_t *)&g_thrd_h->tick)
		& SPT_BUF_TICK_MASK;
	list_vec_id = pthrd_data->data_alloc_out;

	while (list_vec_id != pthrd_data->data_free_in) {
		pnode = (struct spt_buf_list *)vec_id_2_ptr(pclst, list_vec_id);
		if (tick <  pnode->tick)
			tick = tick | (1<<SPT_BUF_TICK_BITS);
		if (tick - pnode->tick < 2) {
			pthrd_data->data_alloc_out = list_vec_id;
			return;
		}
		pdh = (struct spt_dh *)db_id_2_ptr(pclst, pnode->id);
		if (pdh->pdata != NULL) {
			if (spt_data_free_flag(pdh))
				pclst->freedata(pdh->pdata);
		}

		db_free(pclst, pnode->id);
		tmp_id = list_vec_id;
		list_vec_id = pnode->next;
		vec_free(pclst, tmp_id);
		pthrd_data->vec_cnt--;
		pthrd_data->data_list_cnt--;
		pthrd_data->data_cnt--;
	}
	pthrd_data->data_alloc_out = list_vec_id;
}
/* free vector into buffer list */
void vec_free_to_buf_simple(struct cluster_head_t *pclst, int id, int thread_id)
{
	struct spt_thrd_data *pthrd_data = &pclst->thrd_data[thread_id];
	u32 list_vec_id;
	u32 tick;
	struct spt_buf_list *pnode;
	struct spt_vec *pvec;

	pvec = (struct spt_vec *)vec_id_2_ptr(pclst, id);
	pvec->status = SPT_VEC_RAW;
	list_vec_id = vec_alloc_from_rsvlist(pclst, thread_id,
		(struct spt_vec **)&pnode);
	tick = atomic_add_return(0, (atomic_t *)&g_thrd_h->tick)
		& SPT_BUF_TICK_MASK;
	pnode->tick = tick;
	pnode->id = id;
	pnode->next = SPT_NULL;
	if (pthrd_data->vec_free_in == SPT_NULL) {
		pthrd_data->vec_free_in
			= pthrd_data->vec_alloc_out
			= list_vec_id;
	} else {
		pnode = (struct spt_buf_list *)vec_id_2_ptr(
			pclst, pthrd_data->vec_free_in);
		pnode->next = list_vec_id;
		pthrd_data->vec_free_in = list_vec_id;
	}
	pthrd_data->vec_list_cnt++;
	pthrd_data->vec_cnt += 2;
}

/* free db into buffer list */
void db_free_to_buf_simple(struct cluster_head_t *pclst, int id, int thread_id)
{
	struct spt_thrd_data *pthrd_data = &pclst->thrd_data[thread_id];
	u32 list_vec_id;
	u32 tick;
	struct spt_buf_list *pnode;

	list_vec_id = vec_alloc_from_rsvlist(pclst,
		thread_id, (struct spt_vec **)&pnode);
	tick = atomic_add_return(0, (atomic_t *)&g_thrd_h->tick)
		& SPT_BUF_TICK_MASK;
	pnode->tick = tick;
	pnode->id = id;
	pnode->next = SPT_NULL;
	if (pthrd_data->data_free_in == SPT_NULL) {
		pthrd_data->data_free_in
			= pthrd_data->data_alloc_out
			= list_vec_id;
	} else {
		pnode = (struct spt_buf_list *)vec_id_2_ptr(
			pclst, pthrd_data->data_free_in);
		pnode->next = list_vec_id;
		pthrd_data->data_free_in = list_vec_id;
	}
	pthrd_data->data_list_cnt++;
	pthrd_data->data_cnt++;
	pthrd_data->vec_cnt++;
	atomic_sub(1, (atomic_t *)&pclst->data_total);
}

/* free db into buffer list
 * if buffer list exceed water mark, free the db that can be freed
 * supplement vector into reserved list
 */
int db_free_to_buf(struct cluster_head_t *pclst, int id, int thread_id)
{
	struct spt_thrd_data *pthrd_data = &pclst->thrd_data[thread_id];
	u32 list_vec_id;
	u32 tick;
	struct spt_buf_list *pnode;

	list_vec_id = vec_alloc_from_rsvlist(pclst,
		thread_id, (struct spt_vec **)&pnode);
	tick = atomic_add_return(0, (atomic_t *)&g_thrd_h->tick)
		& SPT_BUF_TICK_MASK;
	pnode->tick = tick;
	pnode->id = id;
	pnode->next = SPT_NULL;
	if (pthrd_data->data_free_in == SPT_NULL) {
		pthrd_data->data_free_in
			= pthrd_data->data_alloc_out
			= list_vec_id;
	} else {
		pnode = (struct spt_buf_list *)vec_id_2_ptr(pclst,
			pthrd_data->data_free_in);
		pnode->next = list_vec_id;
		pthrd_data->data_free_in = list_vec_id;
	}
	pthrd_data->data_list_cnt++;
	pthrd_data->data_cnt++;
	pthrd_data->vec_cnt++;
	atomic_sub(1, (atomic_t *)&pclst->data_total);

	if (pthrd_data->data_list_cnt
		> SPT_BUF_DATA_WATERMARK) {
		db_buf_free(pclst, thread_id);
	}
	if (atomic_read((atomic_t *)&pthrd_data->data_list_cnt)
		> SPT_BUF_DATA_WATERMARK) {
		fill_in_rsv_list_simple(pclst, 1, thread_id);
		pclst->status = SPT_WAIT_AMT;
		return SPT_WAIT_AMT;
	} else {
		return fill_in_rsv_list_simple(pclst, 1, thread_id);
	}
}
/* alloc vector from buffer list first
 * if return NULL, then alloc from free list
 */
unsigned int vec_alloc_combo(struct cluster_head_t *pclst,
	int thread_id,
	struct spt_vec **vec)
{
	u32 ret;

	ret = vec_alloc_from_buf(pclst, thread_id, vec);
	if (ret == -1)
		ret = vec_alloc(pclst, vec);
	return ret;
}
/* alloc db from buffer list first
 * if return NULL, then alloc from free list
 */
unsigned int data_alloc_combo(struct cluster_head_t *pclst, int thread_id,
		struct spt_dh **db)
{
	u32 ret;

	ret = db_alloc_from_buf(pclst, thread_id, db);
	if (ret == -1) {
		ret = db_alloc(pclst, db);
		if (ret == -1)
			return -1;
	}
	atomic_add(1, (atomic_t *)&pclst->data_total);
	return ret;
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
        vec_a = vec_alloc(pclst, &pvec_a);
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

