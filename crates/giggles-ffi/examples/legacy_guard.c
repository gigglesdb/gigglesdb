/* Run with the retained pre-format2 DLL to check real old-reader fencing. */
#include "giggles.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
int main(int argc,char **argv){
    char json[4096];gd_handle a=0,b=0;uint32_t code,want;
    assert(argc==4);want=(uint32_t)strtoul(argv[3],NULL,10);
    snprintf(json,sizeof json,"{\"mode\":\"%s\",\"path\":\"%s\"}",argv[1],argv[2]);
    code=gd_v1_admin_open((const uint8_t*)json,strlen(json),&a);
    printf("old DLL mode=%s code=%u expected=%u\n",argv[1],code,want);assert(code==want);
    if(code==0){
        if(strcmp(argv[1],"create")==0){
            const char *seed="{\"command\":\"seed\",\"key\":{\"tenant\":42,\"row\":1},\"row\":{\"status\":\"Failed\",\"failed_at\":103,\"attempt_count\":0,\"version\":0}}";
            assert(gd_v1_call(a,(const uint8_t*)seed,strlen(seed),&b)==0);assert(gd_v1_close(b)==0);
        }
        assert(gd_v1_close(a)==0);
    }else{assert(a==0);}
    return 0;
}
