# Twitter #dkpol analyse

## Formål:

Analyse af hvad der foregår på #dkpol på Twitter. Eksempler på spørgsmål, der kan besvares:
1. Hvilke medier linker flest brugere til?
2. Hvilke dkpol-users følges af flest dkpol-users?
3. Hvilke clusters af users kan vi se i #dkpol?
4. Hvilke virksomheder fylder mest på #dkpol?

## Forudsætninger:

Automatisk hentning og lagring af alle tweets under hashtagget #dkpol samt statistik og metadata (n_followers, n_tweets, mailadresse, oprettet-dato) og relationer inden for #dkpol.


## Todo:

1. Vælg databasetyper/datalagring
2. Væk rasperry til live
3. Design database (tabeller: tweets, users, userstats, relations, links)
4. Find ud af at konvertere Twitter urls til almindelige urls
5. Lav script, der kan querie twitter for tweets, relationer og stats og lagre i DB
6. Migrer til rasperry-pi og opsæt cronjobs

## På længere sigt
6. Migrer til cloud?
7. Berig med CVR-data? Join på mailaddr
8. Visualiser med D3 og host i cloud? JavaScript-arbejde
