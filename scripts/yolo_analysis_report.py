import psycopg2

def generate_report():
    """Generate answers for report questions"""
    print(" YOLO ANALYSIS REPORT ")
    print("="*60)
    
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="medical_warehouse",
        user="postgres",
        password="031628"
    )
    cur = conn.cursor()
    
    # Question 1: Do "promotional" posts get more views?
    print("\n1. Do 'promotional' posts get more views than 'product_display' posts?")
    cur.execute("""
    SELECT 
        image_category,
        COUNT(*) as post_count,
        AVG(views)::numeric(10,2) as avg_views,
        AVG(forwards)::numeric(10,2) as avg_forwards
    FROM analytics_marts.fct_image_detections
    WHERE image_category IN ('promotional', 'product_display')
    GROUP BY image_category
    ORDER BY avg_views DESC;
    """)
    
    results = cur.fetchall()
    if len(results) >= 2:
        prom_views = results[0][2]
        prod_views = results[1][2]
        difference = prom_views - prod_views
        
        print(f"   • Promotional posts: {prom_views} avg views")
        print(f"   • Product display posts: {prod_views} avg views")
        print(f"   • Difference: {difference:.2f} views")
        
        if difference > 0:
            print(f"    CONCLUSION: YES, promotional posts get {difference:.2f} more average views")
        else:
            print(f"    CONCLUSION: NO, product display posts get better engagement")
    
    # Question 2: Which channels use more visual content?
    print("\n2. Which channels use more visual content?")
    cur.execute("""
    SELECT 
        c.channel_name,
        c.channel_type,
        COUNT(f.message_id) as total_messages,
        SUM(CASE WHEN f.has_image THEN 1 ELSE 0 END) as messages_with_images,
        ROUND((SUM(CASE WHEN f.has_image THEN 1 ELSE 0 END) * 100.0 / COUNT(f.message_id)), 2) as image_percentage
    FROM analytics_marts.fct_messages f
    JOIN analytics_marts.dim_channels c ON f.channel_key = c.channel_key
    GROUP BY c.channel_name, c.channel_type
    ORDER BY image_percentage DESC;
    """)
    
    print("   Channel Visual Content Usage:")
    for row in cur.fetchall():
        print(f"   • {row[0]} ({row[1]}): {row[3]}/{row[2]} messages ({row[4]}%)")
    
    # Question 3: Limitations of pre-trained models
    print("\n3. What are the limitations of using pre-trained YOLO for medical products?")
    print("""
   LIMITATIONS:
   1. Generic categories: YOLO is trained on COCO dataset (80 general objects)
   2. No medical specificity: Can't distinguish 'medicine bottle' from 'water bottle'
   3. Language bias: Trained on Western contexts, may miss Ethiopian medical products
   4. Product recognition: Can detect 'bottle' but not what's inside (pills, liquid medicine)
   5. Text in images: Can't read product names or dosages in images
   
   SOLUTIONS NEEDED:
   • Fine-tune YOLO on medical product dataset
   • Add OCR for text extraction
   • Custom training on Ethiopian medical products
   • Combine with text analysis from message content
    """)
    
    #  Most detected objects
    print("\n Most Common Objects Detected")
    cur.execute("""
    SELECT 
        primary_object,
        COUNT(*) as frequency
    FROM image_analysis.yolo_detections
    WHERE primary_object != 'none'
    GROUP BY primary_object
    ORDER BY frequency DESC
    LIMIT 10;
    """)
    
    print("   Top objects detected:")
    for i, (obj, freq) in enumerate(cur.fetchall(), 1):
        print(f"   {i}. {obj}: {freq} images")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    generate_report()