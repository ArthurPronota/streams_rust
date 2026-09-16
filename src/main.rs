use tokio_stream::StreamExt ;

#[tokio::main]
async fn main() {
    let mut s = tokio_stream::iter(vec![1,2,3,4,5]) 
            .filter(|x| x % 2 == 0) ;

    while let Some(v)= s.next().await {
        println!("{v}") ; /* Out:
        2
        4
         */
    }
}
