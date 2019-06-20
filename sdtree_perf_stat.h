#ifndef _SDTREE_PERF_STAT_H_
#define _SDTREE_PERF_STAT_H_

#define THREAD_NUM 8
#define _PERF_STAT_
typedef struct sd_perf_stat_s
{
    char *name;
    unsigned long long  total[THREAD_NUM];
    unsigned long long  cur[THREAD_NUM];
    unsigned long long  max[THREAD_NUM];
    unsigned long long  min[THREAD_NUM];
    unsigned long long  cnt[THREAD_NUM];
}sd_perf_stat;

#define PERF_STAT_DEFINE(x) \
    sd_perf_stat perf_##x = \
    { \
        .name = #x, \
        .total = {0}, \
        .max = {0}, \
        .min = {0}, \
        .cnt = {0}, \
    };

#define PERF_STAT_PTR(x) &perf_##x
#ifdef _PERF_STAT_
#define PERF_STAT_START(x) perf_##x.cur[g_thrd_id] = rdtsc()

#define PERF_STAT_END(x) \
    { \
        unsigned long long end,total; \
        end = rdtsc(); \
        total = end - perf_##x.cur[g_thrd_id]; \
        perf_##x.total[g_thrd_id] += total; \
        if(perf_##x.min[g_thrd_id] > total) \
            perf_##x.min[g_thrd_id] = total; \
        if(perf_##x.max[g_thrd_id] < total) \
            perf_##x.max[g_thrd_id] = total; \
        perf_##x.cnt[g_thrd_id]++; \
    }
#else
#define PERF_STAT_START(x) 
#define PERF_STAT_END(x) 

#endif
#define PERF_STAT_DEC(x) \
    extern sd_perf_stat perf_##x

PERF_STAT_DEC(whole_insert);
PERF_STAT_DEC(insert_up_rd);
PERF_STAT_DEC(insert_down_rd);
PERF_STAT_DEC(insert_last_down);
PERF_STAT_DEC(insert_up_down);
PERF_STAT_DEC(insert_first_set);
PERF_STAT_DEC(whole_delete);
PERF_STAT_DEC(whole_query_by_hash);
PERF_STAT_DEC(leaf_data_prediction_vec);
PERF_STAT_DEC(leaf_data_check_vec);
PERF_STAT_DEC(calc_hash_start_vec);
PERF_STAT_DEC(spt_cluster_scan_perf);
PERF_STAT_DEC(rbtree_scan_perf);
PERF_STAT_DEC(rbtree_scan_perf);
PERF_STAT_DEC(find_vec_from_module);
PERF_STAT_DEC(find_vec_from_module1);
PERF_STAT_DEC(find_stable_tree);
PERF_STAT_DEC(scan_grp_vec);
PERF_STAT_DEC(module_tree_get_data);
PERF_STAT_DEC(test_delete_data);
PERF_STAT_DEC(final_find_data);
#endif
