#include "TypeDef.hpp"

#include "Global.hpp"


template<>
LatentVector<double, LATENT_FACTOR_K> _abs(LatentVector<double, LATENT_FACTOR_K> input) {
	return input.Abs();
}
template<>
LatentVector<float, LATENT_FACTOR_K> _abs(LatentVector<float, LATENT_FACTOR_K> input) {
	return input.Abs();
}
template<>
LatentVector<int32_t, LATENT_FACTOR_K> _abs(LatentVector<int32_t, LATENT_FACTOR_K> input) {
	return input.Abs();
}
template<>
LatentVector<int64_t, LATENT_FACTOR_K> _abs(LatentVector<int64_t, LATENT_FACTOR_K> input) {
	return input.Abs();
}

/*
template <typename T>
T idenelem(Op operation) {
	switch(operation) {
	case PLUS:
		return 0;
	case MULTIPLY:
		return 1;
	case LOR:
		return false;
	case LAND:
		return true;
	case MAX:
		return std::numeric_limits<T>::min();
	case MIN:
		return std::numeric_limits<T>::max();
	default:
        T t;
        return t;
	}
}
*/
//XXX - tslee: 3/5 2018

template <>
LatentVector<double, LATENT_FACTOR_K> idenelem(Op operation)
{
	LatentVector<double, LATENT_FACTOR_K> k;
	switch(operation){
		case PLUS:
			k.SetValue(0.0);
#ifndef LATENT_VECTOR_WO_BIAS
			k.SetBias(0.0);
#endif
			return k;
		case MULTIPLY:
			k.SetValue(1.0);
#ifndef LATENT_VECTOR_WO_BIAS
			k.SetBias(0.0);
#endif
			return k;
		case LOR:
			k.SetValue(0.0);
			return k;
		case LAND:
			k.SetValue(1.0);
			return k;
		case MAX:
			k.SetValue(std::numeric_limits<double>::min());
			return k;
		case MIN:
			k.SetValue(std::numeric_limits<double>::max());
			return k;
		default:
            return k;
			//std::cout << "NOT DEFINED"<<std::endl;
			//abort();
	}
}
template <>
LatentVector<float, LATENT_FACTOR_K> idenelem(Op operation)
{
	LatentVector<float, LATENT_FACTOR_K> k;
	switch(operation){
		case PLUS:
			k.SetValue(0.0);
#ifndef LATENT_VECTOR_WO_BIAS
			k.SetBias(0.0);
#endif
			return k;
		case MULTIPLY:
			k.SetValue(1.0);
#ifndef LATENT_VECTOR_WO_BIAS
			k.SetBias(0.0);
#endif
			return k;
		case LOR:
			k.SetValue(0.0);
			return k;
		case LAND:
			k.SetValue(1.0);
			return k;
		case MAX:
			k.SetValue(std::numeric_limits<float>::min());
			return k;
		case MIN:
			k.SetValue(std::numeric_limits<float>::max());
			return k;
		default:
            return k;
			//std::cout << "NOT DEFINED"<<std::endl;
			//abort();
	}
}

template <>
LatentVector<int32_t, LATENT_FACTOR_K> idenelem(Op operation)
{
	LatentVector<int32_t, LATENT_FACTOR_K> k;
	switch(operation){
		case PLUS:
			k.SetValue(0);
#ifndef LATENT_VECTOR_WO_BIAS
			k.SetBias(0);
#endif
			return k;
		case MULTIPLY:
			k.SetValue(1);
#ifndef LATENT_VECTOR_WO_BIAS
			k.SetBias(0);
#endif
			return k;
		case LOR:
			k.SetValue(0);
			return k;
		case LAND:
			k.SetValue(1);
			return k;
		case MAX:
			k.SetValue(-16777216);
			return k;
		case MIN:
			k.SetValue(16777215);
			return k;
		default:
            return k;
			//std::cout << "NOT DEFINED"<<std::endl;
			//abort();
	}
}

template <>
LatentVector<int64_t, LATENT_FACTOR_K> idenelem(Op operation)
{
	LatentVector<int64_t, LATENT_FACTOR_K> k;
	switch(operation){
		case PLUS:
			k.SetValue(0);
#ifndef LATENT_VECTOR_WO_BIAS
			k.SetBias(0);
#endif
			return k;
		case MULTIPLY:
			k.SetValue(1);
#ifndef LATENT_VECTOR_WO_BIAS
			k.SetBias(0);
#endif
			return k;
		case LOR:
			k.SetValue(0);
			return k;
		case LAND:
			k.SetValue(1);
			return k;
		case MAX:
			k.SetValue(-281474976710656);
			return k;
		case MIN:
			k.SetValue(281474976710655);
			return k;
		default:
            return k;
			//std::cout << "NOT DEFINED"<<std::endl;
			//abort();
	}
}


template<typename T, int K>
void LatentVector<T,K>::SetRandomValue(pcg64_fast* rand_gen_) {
	for (auto i = 0; i < K; i++) {
		k[i] = 2 * (T)(((double)((*rand_gen_)() % UserArguments::LATENT_RANDOM_NUM_DECIMAL_PLACE + 1) / UserArguments::LATENT_RANDOM_NUM_DECIMAL_PLACE) * UserArguments::LATENT_RANDOM_MAX_VALUE) - 1;
	}

}
