
#include "bitops.h"


static unsigned long _find_next_bit(const unsigned long *addr,
		unsigned long nbits, unsigned long start, unsigned long invert)
{
	unsigned long tmp;

	if (!nbits || start >= nbits)
		return nbits;

	tmp = addr[start / BITS_PER_LONG] ^ invert;

	/* Handle 1st word. */
	tmp &= BITMAP_FIRST_WORD_MASK(start);
	start = round_down(start, BITS_PER_LONG);

	while (!tmp) {
		start += BITS_PER_LONG;
		if (start >= nbits)
			return nbits;

		tmp = addr[start / BITS_PER_LONG] ^ invert;
	}

	return min(start + __ffs(tmp), nbits);
}

/*
 * Find the next set bit in a memory region.
 */
unsigned long find_next_bit(const unsigned long *addr, unsigned long size,
			    unsigned long offset)
{
	return _find_next_bit(addr, size, offset, 0UL);
}

unsigned long find_next_zero_bit(const unsigned long *addr, unsigned long size,
				 unsigned long offset)
{
	return _find_next_bit(addr, size, offset, ~0UL);
}



#if 1



		#if 0
		int vec_alloc_interval(cluster_head_t *pclst, spt_vec **vec)
		{
			char *grp;
			u64 va_old, va_new;
			int fs, gid;
			gid = id;
				
		re_alloc:
			grp  = grp_id_2_ptr(pclst, gid);
		    while(1)
		    {
		        va_old = *(u64 *)grp;
				if((va_old & GRP_ALLOCMAP_MASK) == 0)
				{
					if(gid-id >= GRPS_PER_PG)
					{
						vec = NULL;
						return -1;
					}
					gid++;
					goto re_alloc;
				}
				fs = find_next_bit(grp, 30, 0);
				if(fs >=30 )
				{
					if(gid-id >= GRPS_PER_PG)
					{
						vec = NULL;
						return -1;
					}
					gid++;
					goto re_alloc;
				}
				va_new = va_old & (~(1 << fs));
				
		        if(va_old  == atomic64_cmpxchg((atomic64_t *)grp, va_old,va_new))
		            break;
		    }
			*vec = (spt_vec *)(grp + fs*sizeof(spt_vec));
			return gid*VEC_PER_GRP + fs;
		}

		int db_alloc_interval(cluster_head_t *pclst, spt_dh **db)
		{
			char *grp;
			u64 va_old, va_new;
			int fs, ns, gid;
			gid = pclst->grpid;
			
		re_alloc:
			grp  = grp_id_2_ptr(pclst, gid);
			while(1)
			{
				va_old = atomic64_read((atomic64_t *)grp);
				if((va_old & GRP_ALLOCMAP_MASK) == 0)
				{
					if(gid-id >= 1)
					{
						db = NULL;
						spt_assert(0);
						return -1;
					}
					gid++;
					goto re_alloc;
				}
				fs = find_next_bit(grp, 30, 0);
				while(1)
				{
					if(fs >=29 )
					{
						if(gid-id >= GRPS_PER_PG)
						{
							db = NULL;
							return -1;
						}
						gid++;
						goto re_alloc;
					}
					ns = find_next_bit(grp, 30-fs-1, fs+1);
					if(ns == fs+1)
						break;
					fs = ns;
				}
				va_new = va_old & (~(3 << fs));
				
				if(va_old  == atomic64_cmpxchg((atomic64_t *)grp, va_old,va_new))
					break;
			}
			*db = (spt_dh *)(grp + fs*sizeof(spt_vec));
			return gid*VEC_PER_GRP + fs;
		}
		#endif

#endif
