# Analyse des résultats et conclusions

Le dashboard couvre l'activité des taxis à Chicago entre juillet et décembre 2023. Voici ce qui en ressort une fois qu'on croise les trois angles disponibles : le temporel, la durée des trajets et la géographie.

## 1. Trajets et revenus ne bougent pas toujours ensemble

Première chose frappante en regardant les courbes jour par jour : plus de trajets ne veut pas dire plus d'argent généré. Le 12 octobre 2023 est la journée la plus rentable de toute la période, avec 21 564 trajets pour environ 587 018 $ de revenus. Mais trois jours plus tôt, le 9 octobre, on observe presque autant d'activité (18 006 trajets) pour un revenu quasi identique, plus de 571 000 $ avec un panier moyen par trajet nettement plus élevé (31,82 $).

Autrement dit, une journée avec moins de courses peut rapporter presque autant, simplement parce que les trajets sont en moyenne plus longs ou plus chers. C'est le genre de nuance qu'on rate si on ne regarde que le nombre de trajets sans le croiser avec le revenu moyen.

### Le marathon, un cas d'école

Ces deux journées ne sont pas un hasard : le Chicago Marathon a eu lieu le 8 octobre 2023, et l'activité reste élevée plusieurs jours autour de l'événement. Ça colle avec l'intuition qu'un tel événement draine du monde, des déplacements avant/après la course, et probablement des visiteurs venus d'ailleurs.

Je reste prudent sur la causalité, cela dit. Les données montrent une activité forte sur cette période, mais rien ne prouve que tout est imputable au marathon plutôt qu'à d'autres facteurs concomitants (un week-end, la météo, etc.). C'est une corrélation intéressante à noter, pas une preuve.

## 2. Un ralentissement net en fin de période

L'activité redescend vers la fin de l'année. Plusieurs explications possibles viennent à l'esprit : la saisonnalité hivernale, des conditions météo moins favorables aux déplacements, ou un changement de rythme de vie à l'approche des fêtes (une partie des habitants et visiteurs quittant temporairement la ville).

Impossible de trancher avec les données actuelles, cependant. Le snapshot s'arrête au 13 décembre 2023, donc avant Noël et le Nouvel An, deux périodes qui pourraient au contraire faire remonter l'activité (déplacements vers les aéroports, festivités). Pour vraiment comprendre la saisonnalité, il faudrait soit un décembre complet, soit plusieurs années de données à comparer.

## 3. La moyenne cache plus qu'elle ne montre

La durée moyenne d'un trajet, tous jours confondus, tourne autour de 20,45 minutes. Un chiffre correct, mais qui masque une réalité plus contrastée dès qu'on regarde jour par jour.

Le 9 novembre 2023 illustre bien le problème : la moyenne affiche 23,63 minutes, mais la médiane n'est que de 16 minutes, et le P90 grimpe à 52,65 minutes. Concrètement, la majorité des trajets ce jour-là restent courts, mais une minorité de trajets très longs tire la moyenne vers le haut.

Plusieurs situations réelles peuvent expliquer ces trajets longs : un aller-retour vers un aéroport, un trajet entre deux quartiers éloignés, les heures de pointe, ou encore un événement qui ferme des rues et rallonge les trajets (le marathon en est justement un bon exemple, avec ses restrictions de circulation sur une partie du parcours). Le P90 est précieux ici : il permet de repérer les journées où ces trajets longs pèsent réellement, ce que la moyenne seule ne dit pas.

## 4. Les zones ne se ressemblent pas du tout

En regardant les Community Areas, trois zones se détachent nettement en volume de trajets :

| Zone | Trajets | Revenu moyen | Distance moyenne |
|---|---|---|---|
| Near North Side | 637 353 | 17,06 $ | 3,32 miles |
| O'Hare | 496 486 | 53,04 $ | 14,22 miles |
| The Loop | 485 427 | 17,33 $ | 3,42 miles |
| Garfield Ridge | 98 853 | 43,67 $ | 10,50 miles |

**Near North Side** arrive en tête, sans surprise : c'est un quartier dense en activité commerciale, touristique et hôtelière, donc logiquement beaucoup de déplacements courts. Le revenu moyen par trajet reste faible (17,06 $), cohérent avec une distance moyenne de seulement 3,32 miles.

**The Loop**, le centre d'affaires de Chicago, suit un profil quasi identique : beaucoup de trajets (485 427), un revenu moyen comparable (17,33 $) et des distances tout aussi courtes (3,42 miles). Deux zones à fort volume mais à faible valeur unitaire.

**O'Hare** casse complètement ce schéma. Moins de trajets que les deux zones précédentes (496 486), mais un revenu moyen presque trois fois plus élevé (53,04 $) et une distance moyenne de 14,22 miles. Logique : les trajets liés à l'aéroport relient souvent des points bien plus éloignés que les déplacements intra-centre-ville.

## 5. Volume et valeur, deux histoires différentes

C'est sans doute le point le plus utile de toute cette analyse : le nombre de trajets et le revenu par trajet racontent deux histoires distinctes. Near North Side génère le plus de trajets, mais avec un panier moyen de 17 $. O'Hare fait moins de trajets, mais chacun rapporte trois fois plus.

Garfield Ridge confirme ce schéma à plus petite échelle : nettement moins de trajets que les trois grosses zones (98 853), mais un revenu moyen élevé (43,67 $) porté par des distances longues (10,50 miles) — probablement une zone périphérique avec ses propres dynamiques de déplacement longue distance.

Bref, se limiter au nombre de trajets pour juger l'activité d'une zone donne une image incomplète. Il faut croiser volume, revenu moyen et distance pour vraiment comprendre ce qui se passe.

## 6. Pour aller plus loin

Cette première analyse ouvre plusieurs pistes qu'il serait intéressant de creuser en croisant les données taxi avec :

- les horaires de vols des aéroports (pour mieux comprendre les pics à O'Hare) ;
- les données météo ;
- le calendrier des événements sportifs et culturels ;
- les jours fériés ;
- des données de fréquentation touristique ;
- les données de circulation routière.

Ça permettrait de vérifier si les variations observées sont vraiment liées à ces facteurs plutôt qu'à une simple coïncidence de calendrier.

## Conclusion

L'activité des taxis à Chicago dépend fortement de trois choses : la période de l'année, la durée réelle des trajets, et la zone géographique concernée. Les événements comme le marathon semblent avoir un effet notable sur l'activité, même si la causalité exacte reste à confirmer. La distinction moyenne/médiane/P90 s'est révélée indispensable pour ne pas se laisser tromper par des moyennes qui lissent des réalités très différentes selon les jours.

Et surtout, l'analyse géographique montre qu'une zone très fréquentée n'est pas forcément celle qui rapporte le plus par trajet. Near North Side et The Loop concentrent le volume, O'Hare concentre la valeur. C'est exactement ce genre de nuance qu'une architecture data bien construite permet de faire émerger à partir de données brutes autrement difficiles à exploiter.
