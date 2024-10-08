---
title: Staging Dashboard
---

```js echo
const wdiTopCountries = FileAttachment("data/masterpy.csv").csv({typed: true});
```

```js echo
display(Inputs.table(wdiTopCountries, {width:300}));
```

``` js echo
const missing = FileAttachment("data/missingLoader.csv").csv({typed: true});
Inputs.table(missing, {width:300})
```