/* SPDX-License-Identifier: LGPL-2.1-or-later */
#ifndef GIGGLES_GLIBM_H
#define GIGGLES_GLIBM_H
#ifdef __cplusplus
extern "C" {
#endif
const char *giggles_glibm_identity(void);
#define GIGGLES_GLIBM_UNARY(f) double giggles_glibm_##f(double x)
GIGGLES_GLIBM_UNARY(cbrt);
GIGGLES_GLIBM_UNARY(exp);
GIGGLES_GLIBM_UNARY(log);
GIGGLES_GLIBM_UNARY(log10);
GIGGLES_GLIBM_UNARY(sin);
GIGGLES_GLIBM_UNARY(cos);
GIGGLES_GLIBM_UNARY(tan);
GIGGLES_GLIBM_UNARY(asin);
GIGGLES_GLIBM_UNARY(acos);
GIGGLES_GLIBM_UNARY(atan);
GIGGLES_GLIBM_UNARY(sinh);
GIGGLES_GLIBM_UNARY(cosh);
GIGGLES_GLIBM_UNARY(tanh);
GIGGLES_GLIBM_UNARY(asinh);
GIGGLES_GLIBM_UNARY(acosh);
GIGGLES_GLIBM_UNARY(atanh);
GIGGLES_GLIBM_UNARY(erf);
GIGGLES_GLIBM_UNARY(erfc);
GIGGLES_GLIBM_UNARY(tgamma);
GIGGLES_GLIBM_UNARY(lgamma);
#undef GIGGLES_GLIBM_UNARY
double giggles_glibm_pow(double x, double y);
double giggles_glibm_atan2(double y, double x);
#ifdef __cplusplus
}
#endif
#endif
