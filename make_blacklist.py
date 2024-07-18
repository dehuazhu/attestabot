def main():
    outfile = 'blacklist.txt'

    word_lists = (
            'words_deu.txt',
            'words_eng.txt',
            'words_fra.txt',
            'words_ita.txt',
            )

    with open(outfile, 'w') as out:
        for wl in word_lists:
            with open(wl) as infile:
                for word in infile.readlines():
                    if 'iso' in word and word.strip()!='iso':
                        out.write(word)

if __name__=='__main__':
    main()
