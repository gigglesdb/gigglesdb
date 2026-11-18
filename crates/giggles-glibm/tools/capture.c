/* Capture the running image's scalar libm, without compiler builtins.
   SPDX-License-Identifier: LGPL-2.1-or-later */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <gnu/libc-version.h>
static const char *names[] = {"cbrt","exp","log","log10","sin","cos","tan","asin","acos","atan","sinh","cosh","tanh","asinh","acosh","atanh","erf","erfc","tgamma","lgamma","pow","atan2"};
static uint64_t state = UINT64_C(0x243f6a8885a308d3);
static uint64_t random64(void) {
    state ^= state >> 12; state ^= state << 25; state ^= state >> 27;
    return state * UINT64_C(2685821657736338717);
}
static double value(uint64_t u) { double x; memcpy(&x,&u,8); return x; }
static uint64_t bits(double x) { uint64_t u; memcpy(&u,&x,8); return u; }
static void put(FILE *f, uint64_t u) {
    unsigned char b[8]; for (int i=0;i<8;i++) b[i]=(unsigned char)(u>>(8*i));
    if (fwrite(b,1,8,f)!=8) { perror("write"); exit(2); }
}
static double input(size_t i, int n) {
    static const uint64_t edge[] = {0,UINT64_C(0x8000000000000000),1,UINT64_C(0x8000000000000001),UINT64_C(0x000fffffffffffff),UINT64_C(0x0010000000000000),UINT64_C(0x3fefffffffffffff),UINT64_C(0x3ff0000000000000),UINT64_C(0x3ff0000000000001),UINT64_C(0xbff0000000000000),UINT64_C(0x7fefffffffffffff),UINT64_C(0xffefffffffffffff),UINT64_C(0x7ff0000000000000),UINT64_C(0xfff0000000000000),UINT64_C(0x7ff8000000000001),UINT64_C(0xfff8000000001234),UINT64_C(0x7ff0000000000001),UINT64_C(0xfff0000000000001)};
    if (i<sizeof(edge)/sizeof(edge[0])) return value(edge[i]);
    if (i<1000) {
        /* Both sides of every gamma pole/near integer, plus powers of two. */
        size_t j=i-sizeof(edge)/sizeof(edge[0]);
        double x=j<600 ? ((double)(j/3)-100.0) : value(((uint64_t)((j-600)/3+900))<<52);
        return value(bits(x)+(j%3)-1);
    }
    uint64_t r=random64();
    if (i%4==0) return value(r); /* log-uniform magnitudes, NaNs, infinities */
    double u=(double)(r>>11)*0x1p-53;
    if (i%4==1) { /* one-ulp neighborhoods of branch boundaries/integers */
        static const double boundary[]={-745,-710,-56,-28,-22,-2,-1,-0.5,0,0.5,1,2,8,22,28,56,171,709,710};
        double x=boundary[(i/4)%(sizeof(boundary)/sizeof(boundary[0]))];
        return value(bits(x)+(r%9)-4);
    }
    if (n==7||n==8||n==15) return 2*u-1;
    if (n==14) return 1+1000*u;
    if (n==1||n==10||n==11) return 1500*u-750;
    if (n==18||n==19) return 400*u-200;
    if (n==2||n==3) return 1000*u;
    return 2000*u-1000;
}
int main(int argc,char **argv) {
    void *lib=dlopen("libm.so.6",RTLD_NOW); if(!lib) return 2;
    void *fn[22]; for(int n=0;n<22;n++) {
        fn[n]=dlsym(lib,names[n]); if(!fn[n]) return 2;
        Dl_info info; dladdr(fn[n],&info);
        fprintf(stderr,"%s offset=0x%lx library=%s glibc=%s\n",names[n],(unsigned long)((char*)fn[n]-(char*)info.dli_fbase),info.dli_fname,gnu_get_libc_version());
    }
    const char *internal[]={"__exp_finite","__log_finite","__pow_finite"};
    for(int n=0;n<3;n++) {
        void *p=dlvsym(lib,internal[n],"GLIBC_2.15"); Dl_info info;
        if(p && dladdr(p,&info)) fprintf(stderr,"%s resolved=0x%lx\n",internal[n],(unsigned long)((char*)p-(char*)info.dli_fbase));
    }
    if(argc==2 && !strcmp(argv[1],"--query")) {
        char name[32]; unsigned long long a,b;
        while(scanf("%31s %llx %llx",name,&a,&b)==3) {
            int n; for(n=0;n<22;n++) if(!strcmp(name,names[n])) break;
            if(n==22) return 2;
            double z=n<20?((double(*)(double))fn[n])(value(a)):((double(*)(double,double))fn[n])(value(a),value(b));
            printf("%016llx\n",(unsigned long long)bits(z));
        }
        return 0;
    }
    if(argc!=3) { fprintf(stderr,"usage: capture DIR COUNT | --query\n"); return 2; }
    size_t count=strtoull(argv[2],0,10);
    for(int n=0;n<22;n++) {
        char path[4096]; snprintf(path,sizeof(path),"%s/%s.bin",argv[1],names[n]);
        FILE *f=fopen(path,"wb"); if(!f) {perror(path);return 2;}
        state=UINT64_C(0x243f6a8885a308d3); /* reproducible across runs */
        for(size_t i=0;i<count;i++) {
            double x=input(i,n),y=input(i+31,n);
            if(n>=20 && i<18*18) {x=input(i/18,n);y=input(i%18,n);}
            double z=n<20?((double(*)(double))fn[n])(x):((double(*)(double,double))fn[n])(x,y);
            put(f,bits(x)); if(n>=20) put(f,bits(y)); put(f,bits(z));
        }
        if(fclose(f)) return 2;
        fprintf(stderr,"captured %s %zu\n",names[n],count);
    }
    return 0;
}
