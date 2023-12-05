import {Component, OnDestroy, OnInit} from '@angular/core';
import pako from 'pako';
import Papa from 'papaparse';
import {filter, map, mergeMap, Observable, ReplaySubject, Subscription, tap} from 'rxjs';

@Component({
    selector: 'app-root',
    templateUrl: './app.component.html',
    styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnDestroy, OnInit {
    private subscriptions = new Subscription();

    onFileSelected = new ReplaySubject<File>(1)

    decompressedData: string = '';


    ngOnDestroy(): void {
        this.subscriptions.unsubscribe();
    }

    ngOnInit(): void {
        this.subscriptions.add(
            this.onFileSelected
                .pipe(
                    tap((file) => console.debug(`Loading file ${file.name}`)),
                    mergeMap((file) => {
                        const filename = file.name;
                        const extension = filename.split('.').pop();

                        if (extension === 'gz') {
                            return this.readAsArrayBuffer(file).pipe(
                                map((buffer) => {
                                    const binary = new Uint8Array(buffer)
                                    return pako.inflate(binary, {to: 'string'});
                                })
                            );
                        } else {
                            return this.readAsText(file)
                        }
                    }),
                    tap((ignored) => console.debug('File loaded.')),
                    map((data) => this.swapDelimitersAddHeaders(data)),
                    mergeMap((data) => this.parseAndMungeCsvData(data)),
                )
                .subscribe((file) => {
                    console.log(file);
                    this.decompressedData = JSON.stringify(file, null, 2);
                })
        )
    }

    onFileSelectedClick(event: Event): void {
        const input = event.target as HTMLInputElement;
        if (input.files && input.files.length) {
            this.onFileSelected.next(input.files[0]);
        }
    }


    /**
     * This method swaps the commas in with a delimiter that's not used in the JSON data. Then, it adds headers to the
     * first line so that the CSV parsing library can hydrate each row into a JSON object.
     *
     * The CSV file Snagajob provided was exported from Snowflake and has the following columns:
     *      date – The application date. A full timestamp available in the raw data
     *      hiringManagerId – The same for every application in the file. Basically meaningless.
     *      applicationId – Was important. But, now that the Snagajob divorce is finalized, we have nothing to
     *                      cross-reference against. So, basically meaningless.
     *      document – This is the JSON data that contains the full and raw application data. It's a little funky
     *                 because it's double escaped. First, since it was stored in Snowflake. Second, because Snagajob
     *                 exported this to a CSV for Fourth without first unescaping it.
     *
     * @param data CSV string data
     */
    swapDelimitersAddHeaders(data: string): string {
        let match = null;
        let result = '';

        const regex = /^(.+)$/gm
        while ((match = regex.exec(data)) !== null) {
            result += match[1].replace(/(\d+),(\d+),(\w+),/, "$1|||$2|||$3|||") + "\n";
        }

        // Add the column names and return the result.
        return "date|||hiringManagerId|||applicationId|||document\n" + result
    }

    parseAndMungeCsvData(data: string): Observable<any> {
        return new Observable((observer) => {
            Papa.parse(data, {
                header: true,
                delimiter: '|||',
                fastMode: true,
                skipEmptyLines: true,
                transform: (value: string, field: string | number): any => {
                    // The CSV Snagajob provided has escaped JSON data in it.
                    // It's not clean and must be munged to make it valid.
                    if (field === 'document') {

                        // JSON commas are escaped with a backslash. Remove that.
                        value = value.replaceAll('\\,', ',');

                        // Escaped quotes are actually double escaped. Remove that.
                        value = value.replaceAll('\\\\"', '\\"');

                        const scratch = JSON.parse(value);
                        // console.log(scratch)

                        return {
                            firstName: scratch.Profile.FirstName,
                            lastName: scratch.Profile.LastName,
                            locations: scratch.Locations.map((item: any) => item.Name),
                            postingUrl: scratch.PostingIdString ? 'https://www.snagajob.com/jobs/' + scratch.PostingIdString : undefined,
                            raw: value,
                        }
                    }

                    return value;
                },
                complete: (result) => {
                    observer.next(result.data);
                    observer.complete();
                },
                error: (error: Error) => {
                    observer.error(error);
                }
            });
        })
    }

    readAsArrayBuffer(file: File): Observable<ArrayBuffer> {
        return new Observable((observer) => {
            const reader = new FileReader();

            reader.onload = (event) => {
                if (event.target != null && event.target.result != null) {
                    observer.next(event.target.result as ArrayBuffer);
                    observer.complete();
                } else {
                    observer.error('Unable to read file.');
                }
            };

            reader.onerror = (error) => {
                observer.error(error);
            };

            reader.readAsArrayBuffer(file);

            return () => reader.abort();
        });
    };

    readAsText(file: File): Observable<string> {
        return new Observable((observer) => {
            const reader = new FileReader();

            reader.onload = (event) => {
                if (event.target != null && event.target.result != null) {
                    observer.next(event.target.result.toString());
                    observer.complete();
                } else {
                    observer.error('Unable to read file.');
                }
            };

            reader.onerror = (error) => {
                observer.error(error);
            };

            reader.readAsText(file);

            return () => reader.abort();
        });
    };
}
