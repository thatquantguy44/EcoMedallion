# ECB Eligible-Assets Licensing Review

Review date: 2026-10-24

Dataset: ECB List of eligible marketable assets (MID)

Decision status: **reviewed and verified**

Pipeline owner: **not recorded, by request**

Licensing reviewer: **repository maintainer; identity not recorded, by request**

This record is an operational reuse assessment, not legal advice. It covers the
ECB-published eligible-assets release files obtained from the MID download page
and RSS feed. It does not grant rights in third-party content that the ECB does
not own, and it does not certify a downstream product's regulatory use.

## Primary evidence reviewed

1. [ECB Disclaimer & Copyright](https://www.ecb.europa.eu/services/using-our-site/disclaimer/html/index.en.html)
   permits free use of information obtained directly from the ECB website,
   subject to accurate reproduction and citation of the ECB as the source. If
   that information is incorporated into material offered for sale, customers
   must be told before payment and at each access that it is freely available
   from the ECB website. User modifications must be stated explicitly.
2. [ECB List of eligible marketable assets](https://www.ecb.europa.eu/mopo/coll/assets/html/list-MID.en.html)
   is the official download page for the current full/update files and annual
   archives. Its footer links the ECB disclaimer and copyright terms above;
   the page states no dataset-specific exception to those terms.
3. [ESCB statistics reuse policy](https://www.ecb.europa.eu/stats/ecb_statistics/governance_and_quality_framework/html/usage_policy.pl.html)
   also permits commercial and non-commercial reuse of ESCB statistics with
   attribution, but excludes third-party data and prohibits modifying the
   statistics or metadata. Because the eligible-assets list is collateral
   reference data rather than clearly an ESCB statistical product, the generic
   ECB website terms in item 1 are the controlling basis used here.

## Operational determination

The pipeline may ingest and use the MID eligible-assets data internally. The
repository maintainer accepted this review for external and commercial
redistribution under these controls:

- reproduce ECB-sourced values accurately and identify the European Central
  Bank as the source;
- retain a link to the original ECB eligible-assets page in Gold/export
  metadata;
- explicitly label any transformation, normalization, derived field, or
  modification as the pipeline operator's work rather than the ECB's;
- if an export is sold or included in a paid product, disclose before payment
  and at each access that the underlying ECB information is freely available
  on the ECB website; and
- do not claim rights to third-party material or ECB-authored papers outside
  the reuse permission. Escalate any newly identified third-party field or
  dataset-specific restriction before redistribution.

The pipeline's canonicalization, SCD2 intervals, hashes, gap flags, and derived
change records are modifications/derived work and must be described as such in
external exports. The pipeline must not imply ECB endorsement or that inclusion
in a historical snapshot independently establishes current legal eligibility.

## Sign-off

`config/data_licensing.yml` records `review_status: verified` and a role-based
review attestation. The maintainer explicitly requested that neither a personal
reviewer identity nor a pipeline owner be recorded. Re-review at least every
two years or sooner if the ECB changes its terms.

Reviewer attestation:

> I reviewed the primary evidence above, accept the stated controls for the
> intended use, and approve the ECB eligible-assets licensing entry.

Attested by: **repository maintainer; identity not recorded, by request**

Date: **2026-10-24**
