
1
/
22
1
101 Formulaic Alphas
Zura Kakushadze§†1, Geoffrey Lauprete¶2 and Igor Tulchinsky¶3
§ Quantigic® Solutions LLC,4 1127 High Ridge Road, #135, Stamford, CT 06905
† Free University of Tbilisi, Business School & School of Physics
240, David Agmashenebeli Alley, Tbilisi, 0159, Georgia
¶ WorldQuant LLC, 1700 East Putnam Ave, Third Floor, Old Greenwich, CT 06870
December 9, 2015
“There are two kinds of people in this world:
Those seeking happiness, and bullfighters.”
(Zura Kakushadze, ca. early ’90s)5
Abstract
We present explicit formulas – that are also computer code – for
101 real-life quantitative trading alphas. Their average holding
period approximately ranges 0.6-6.4 days. The average pair-wise
correlation of these alphas is low, 15.9%. The returns are strongly
correlated with volatility, but have no significant dependence on
turnover, directly confirming an earlier result by two of us based
on a more indirect empirical analysis. We further find empirically
that turnover has poor explanatory power for alpha correlations.
1 Zura Kakushadze, Ph.D., is the President and a Co-Founder of Quantigic® Solutions LLC and a Full Professor in the
Business School and the School of Physics at Free University of Tbilisi. Email: zura@quantigic.com
2 Geoffrey Lauprete, Ph.D., is the CIO of WorldQuant LLC. Email: Geoffrey.Lauprete@worldquant.com
3 Igor Tulchinsky, M.S., MBA, is the Founder and CEO of WorldQuant LLC. Email: igort.only@worldquant.com
4 DISCLAIMER: This address is used by the corresponding author for no purpose other than to indicate his
professional affiliation as is customary in publications. In particular, the contents of this paper are not intended as
an investment, legal, tax or any other such advice, and in no way represent views of Quantigic® Solutions LLC, the
website www.quantigic.com or any of their other affiliates.
5 Paraphrasing Blondie’s (Clint Eastwood) one-liners from a great 1966 motion picture The Good, the Bad and the
Ugly (directed by Sergio Leone).
2
1. Introduction
There are two complementary – and in some sense even competing – trends in modern
quantitative trading. On the one hand, more and more market participants (e.g., quantitative
traders, inter alia) employ sophisticated quantitative techniques to mine alphas.6 This results in
ever fainter and more ephemeral alphas. On the other hand, technological advances allow to
essentially automate (much of) the alpha harvesting process. This yields an ever increasing
number of alphas, whose count can be in hundreds of thousands and even millions, and with
the exponentially increasing progress in this field will likely be in billions before we know it…
This proliferation of alphas – albeit mostly faint and ephemeral – allows combining them in
a sophisticated fashion to arrive at a unified “mega-alpha”. It is then this “mega-alpha” that is
actually traded – as opposed to trading individual alphas – with a bonus of automatic internal
crossing of trades (and thereby crucial-for-profitability savings on trading costs, etc.), alpha
portfolio diversification (which hedges against any subset of alphas going bust in any given time
period), and so on. One of the challenges in combining alphas is the usual “too many variables,
too few observations” dilemma. Thus, the alpha sample covariance matrix is badly singular.
Also, naturally, quantitative trading is a secretive field and data and other information from
practitioners is not readily available. This inadvertently creates an enigma around modern
quant trading. E.g., with such a large number of alphas, are they not highly correlated with
each other? What do these alphas look like? Are they mostly based on price and volume data,
mean-reversion, momentum, etc.? How do alpha returns depend on volatility, turnover, etc.?
In a previous paper two of us [Kakushadze and Tulchinsky, 2015] took a step in demystifying
the realm of modern quantitative trading by studying some empirical properties of 4,000 real-
life alphas. In this paper we take another step and present explicit formulas – that are also
computer code – for 101 real-life quant trading alphas. Our formulaic alphas – albeit most are
not necessarily all that “simple” – serve a purpose of giving the reader a glimpse into what
some of the simpler real-life alphas look like.7 It also enables the reader to replicate and test
these alphas on historical data and do new research and other empirical analyses. Hopefully, it
further inspires (young) researchers to come up with new ideas and create their own alphas.
6 “An alpha is a combination of mathematical expressions, computer source code, and configuration parameters
that can be used, in combination with historical data, to make predictions about future movements of various
financial instruments.” [Tulchinsky et al, 2015] Here “alpha” – following the common trader lingo – generally
means any reasonable “expected return” that one may wish to trade on and is not necessarily the same as the
“academic” alpha. In practice, often the detailed information about how alphas are constructed may even not be
available, e.g., the only data available could be the position data, so “alpha” then is a set of instructions to achieve
certain stock (or other instrument) holdings by some times ᡲ⡩, ᡲ⡰, … (e.g., a tickers by holdings matrix for each ᡲう).
7 We picked these alphas largely based on simplicity considerations, so they can be presented within the inherent
limitations of a paper. There also exist myriad other, “non-formulaic” (coded and too-complex-to-present) alphas.
3
We discuss some general features of our formulaic alphas in Section 2. These alphas are
mostly “price-volume” (daily close-to-close returns, open, close, high, low, volume and vwap)
based, albeit “fundamental” input is used in some of the alphas, including one alpha utilizing
market cap, and a number of alphas employing some kind of a binary industry classification
such as GICS, BICS, NAICS, SIC, etc., which are used to industry-neutralize various quantities.8
We discuss empirical properties of our alphas in Section 3 based on data for individual alpha
Sharpe ratio, turnover and cents-per-share, and also on a sample covariance matrix. The
average holding period approximately ranges from 0.6 to 6.4 days. The average (median) pair-
wise correlation of these alphas is low, 15.9% (14.3%). The returns ᡄ are strongly correlated
with the volatility ᡈ, and as in [Kakushadze and Tulchinsky, 2015] we find an empirical scaling
ᡄ ~ ᡈ〥 (1)
with ᡐ ≈ 0.76 for our 101 alphas. Furthermore, we find that the returns have no significant
dependence on the turnover ᡆ. This is a direct confirmation of an earlier result by two of us
[Kakushadze and Tulchinsky, 2015], which is based on a more indirect empirical analysis.9
We further find empirically that the turnover per se has poor explanatory power for alpha
correlations. This is not to say that the turnover does not add value in, e.g., modeling the
covariance matrix via a factor model.10 A more precise statement is that pair-wise correlations
〶〷 of the alphas (ᡡ, ᡢ = 1, … , ᡀ label the ᡀ alphas, ᡡ ≠ ᡢ) are not highly correlated with the
product ln( 〶) ln( 〷), where 〶 = ᡆ〶 / †, and † is an a priori arbitrary normalization constant.11
We briefly conclude in Section 4. Appendix A contains our formulaic alphas with definitions
of the functions, operators and input data used therein. Appendix B contains some legalese.
2. Formulaic Alphas
In this section we describe some general features of our 101 formulaic alphas. The alphas
are proprietary to WorldQuant LLC and are used here with its express permission. We provide
as many details as we possibly can within the constraints imposed by the proprietary nature of
the alphas. The formulaic expressions – that are also computer code – are given in Appendix A.
8 More precisely, depending on the alpha and industry classification used, neutralization can be w.r.t. sectors,
industries, subindustries, etc. – different classifications use different nomenclature for levels of similar granularity.
9 In [Kakushadze and Tulchinsky, 2015] the alpha return volatility was not directly available and was estimated
indirectly based on the Sharpe ratio, cents-per-share and turnover data. Here we use direct realized volatility data.
10 Depending on a construction, a priori the turnover might add value via the specific (idiosyncratic) risk for alphas.
11 Here we use log of the turnover as opposed to the turnover itself as the latter has a skewed, roughly log-normal
distribution, while pair-wise correlations take values in (−1, 1) (in fact, their distribution is tighter – see below).
