#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>
#include "store.h"

void analyze_references_source(){
	init_iterator("CHUNK");

	struct chunk_rec r;
	memset(&r, 0, sizeof(r));

	int total_references = 0; // total duplicate references
	int intra_file = 0; // intra-file redundancy
	int duplicate_file = 0; // duplicate files
	int min_fp = 0; // by minimum fingerprint
	int max_fp = 0; // by maximum fingerprint
	int same_suffix = 0; // by file suffix
	int missed_file = 0; // missed
	while(iterate_chunk(&r) == ITER_CONTINUE){

		total_references += r.rcount - 1;
		intra_file += r.rcount - r.fcount;
		if(r.fcount == 1) // chunks referenced by a single file 
			continue;

		struct file_rec *files[r.fcount];
		int i = 0, j = 0;
		for(; i < r.elem_num; i++){
			if (r.list[i] < 0) continue;
			files[j] = malloc(sizeof(struct file_rec));
			memset(files[j], 0, sizeof(struct file_rec));
			files[j]->fid = r.list[i];
			search_file(files[j++]);
		}
		assert(j == r.fcount);

		/* analyze files */
		for(i=0; i < r.fcount - 1; i++){
			int identical = 0, min_similar = 0, similar = 0, suffix = 0;
			for (j = i + 1; j < r.fcount; j++){
				if(memcmp(files[i]->hash, files[j]->hash, sizeof(files[i]->hash)) 
						== 0){
					identical = 1;
					break;
				}else if(memcmp(files[i]->minhash, files[j]->minhash, 
							sizeof(files[i]->minhash)) == 0){
					min_similar = 1;
				}else if(memcmp(files[i]->maxhash, files[j]->maxhash, 
							sizeof(files[i]->maxhash)) == 0){
					similar = 1;
				}else{
					char suf1[8];
					char suf2[8];
					parse_file_suffix(files[i]->fname, suf1, sizeof(suf1));
					parse_file_suffix(files[j]->fname, suf2, sizeof(suf2));
					if (strcmp(suf1, suf2) == 0) {
						suffix = 1;
					}
				}
			}
			if(identical == 1)
				duplicate_file++;
			else if(min_similar == 1)
				min_fp++;
			else if(similar == 1)
				max_fp++;
			else if(suffix == 1)
				same_suffix++;
			else
				missed_file++;
		}
		for(i = 0; i < r.fcount; i++){
			free_file_rec(files[i]);
		}
	}

	close_iterator("CHUNK");

	assert(total_references == intra_file + duplicate_file + min_fp
			+ max_fp + same_suffix + missed_file);

	fprintf(stderr, "%8s %8s %8s %8s %8s %8s\n", 
			"Intra", "DupFile", "Min", "Max", "Suffix", "Missed");
	fprintf(stdout, "%8.5f %8.5f %8.5f %8.5f %8.5f %8.5f\n", 
			1.0*intra_file/total_references, 
			1.0*duplicate_file/total_references, 
			1.0*min_fp/total_references, 
			1.0*max_fp/total_references, 
			1.0*same_suffix/total_references, 
			1.0*missed_file/total_references);
}


int main(int argc, char *argv[])
{
	open_database("dbhome/");

	analyze_references_source();

	close_database();

	return 0;
}
