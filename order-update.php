<?php
error_reporting(E_ERROR);
date_default_timezone_set('UTC');

require ('..' . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR . 'vendor' . DIRECTORY_SEPARATOR . 'autoload.php');

use Google\Cloud\BigQuery\BigQueryClient;

$headers = getallheaders();
if (!isset($headers['X-Shopify-Shop-Domain'])) {
  die('error');
}

$headers = getallheaders();
$shop = str_replace('.myshopify.com', '', $headers['X-Shopify-Shop-Domain']);

$tokens = array(
  //'donchos-development-store' => 'b3af0f700e1354b97f9d8bc386b2f35186498dc2b867304a8fac45ac40e64518', 
  'ozertyus' => '33b8b83fe868f2f9592e5e011fd6578878c317ba146acf3f8d151e0673a5e898',
  'ozertyuk' => 'a9f9f3b198171e2536e1349f3cc7074da5c5b83e6d82af128ce2f216fd92cc1b',
  'ozertyfr' => 'cd6346b3096cd3321c93a09478e796d77f7e5fa993472cee681910f1c0a014bc',
  'ozertyes' => '1718515fee4642fde57ffb1cf95da35b90aaaa7d8e7263c4e3ad6388cc0e5bba',
  'ozertyit' => '12de48289174b9faf98afa1e7cb300ad52cbcada15237f901303825c2278dcea',
  'ozertyde' => 'c66c6e10cc1f7ca7a95a2cdd7a8a892397df465037c081e47b88010925e8bb33',
  'ozertydk' => 'ff1e607fd3822bbd119d1613f63a23ae2415ea407a8fa9845c4a0a8cdd547982',
  'ozertyno' => 'f557e38e0f8a282d4d418e8bca6cd7c7b444527221b2577303553e20e11c9451',
  'ozertyse' => 'af27cf7c5fcf9a0c03635694429cf690a88655758649fbfc119128ffe96fdd98',
  'ozertyau' => '20ca3ba1f2b9c596a80a8e429ba7a45a2c1de7ca02005a107ca64893da6c8b94',
  'ozertyca' => '8addf8c0c50e2f22c4def4aaf41cd76833cf3b69f81ec0bb951176b83fcd64a6',
  'ouistiprix' => '690e424c1dde868ecfc4e860ba982869c61843b981a7f420930b2090533dde3c',
);
$token = (isset($tokens[$shop]) ? $tokens[$shop] : '');

$hmac_header = $_SERVER['HTTP_X_SHOPIFY_HMAC_SHA256'];
$data = file_get_contents('php://input');
$calculated_hmac = base64_encode(hash_hmac('sha256', $data, $token, true));

if ($hmac_header == $calculated_hmac) {
  $bigQuery = new BigQueryClient([
    'keyFilePath' => 'service-account-key.json',
    'projectId' => 'mojo-dashboard-mysql-gds',
  ]);

  $dataset = $bigQuery->dataset('shopify_orders_analytics');
  $table = $dataset->table('orders');

  $fxRates = [];
  $checkFxRatesQuery = $bigQuery->query(
    "SELECT * FROM `mojo-dashboard-mysql-gds.shopify_orders_analytics.fx_rates_BI_engine` order by `Date` desc LIMIT 1"
  );
  $checkFxRates = $bigQuery->runQuery($checkFxRatesQuery);
  foreach ($checkFxRates as $k => $fxRate) {
    $fxRates = $fxRate;
  }

  //file_put_contents('test.txt', $data);

	$order = json_decode($data);

  $dt = new DateTime($order->created_at, new DateTimeZone('UTC'));
  $dt->setTimezone(new DateTimeZone('Europe/Paris'));
  $created_at = $dt->format('Y-m-d H:i:s');

  $jcheckout_id = '';
  if (!empty($order->note_attributes)) {
    foreach ($order->note_attributes as $k => $attr) {
      if ($attr->name == 'JCHECKOUT ID') {
        $jcheckout_id = $attr->value;
      }
    }
  }

  $discount_code = '';
  if (!empty($order->discount_codes)) {
    $discount_code = $order->discount_codes[0]->code;
  }

  $order_subtotal_usd = $order->subtotal_price;
  $order_tax_amount_usd = $order->total_tax;
  $order_discount_amount_usd = $order->total_discounts;
  $order_total_usd = $order->total_price;
  if ($order->currency != 'USD' AND isset($fxRates[strtoupper($order->currency) . '_to_USD'])) {
    $order_subtotal_usd = round($order_subtotal_usd * $fxRates[strtoupper($order->currency) . '_to_USD'], 2);
    $order_tax_amount_usd = round($order_tax_amount_usd * $fxRates[strtoupper($order->currency) . '_to_USD'], 2);
    $order_discount_amount_usd = round($order_discount_amount_usd * $fxRates[strtoupper($order->currency) . '_to_USD'], 2);
    $order_total_usd = round($order_total_usd * $fxRates[strtoupper($order->currency) . '_to_USD'], 2);
  }

  $checkOrderQuery = $bigQuery->query(
    "SELECT `order_name` FROM `mojo-dashboard-mysql-gds.shopify_orders_analytics.orders` WHERE `order_id`=" . $order->id . " LIMIT 1"
  );
  $checkOrder = $bigQuery->runQuery($checkOrderQuery);
  $checkOrderRows = 0;
  foreach ($checkOrder as $row) {
    $checkOrderRows++;
  }
  if ($checkOrderRows > 0) {
    $fulfilment = $fulfilment_at = '';
    if (!empty($order->fulfillments)) {
      $fulfilment = end($order->fulfillments);

      $dt2 = new DateTime($fulfilment->created_at, new DateTimeZone('UTC'));
      $dt2->setTimezone(new DateTimeZone('Europe/Paris'));
      $fulfilment_at = $dt2->format('Y-m-d H:i:s');
    }

    $cancelled_at = '';
    if (!empty($order->cancelled_at)) {
      $dt3 = new DateTime($order->cancelled_at, new DateTimeZone('UTC'));
      $dt3->setTimezone(new DateTimeZone('Europe/Paris'));
      $cancelled_at = $dt3->format('Y-m-d H:i:s');
    }

    $updateOrder = $bigQuery->query("UPDATE `mojo-dashboard-mysql-gds.shopify_orders_analytics.orders` SET
      `financial_status`='" . addslashes(htmlspecialchars($order->financial_status)) . "', 
      `subtotal`=" . $order->subtotal_price . ", 
      `tax_amount`=" . $order->total_tax . ", 
      `discount_code`='" . addslashes(htmlspecialchars($discount_code)) . "', 
      `discount_amount`=" . $order->total_discounts . ", 
      `total`=" . $order->total_price . ", 
      " . ($order->fulfillment_status != '' ? "`fulfillment_status`='" . addslashes(htmlspecialchars($order->fulfillment_status)) . "', " : "") . "
      " . ($fulfilment_at != '' ? "`fulfillment_at`='" . addslashes(htmlspecialchars($fulfilment_at)) . "', " : "") . "
      " . ($cancelled_at != '' ? "`cancelled_at`='" . addslashes(htmlspecialchars($cancelled_at)) . "', " : "") . "
      `subtotal_usd`=" . $order_subtotal_usd . ", 
      `tax_amount_usd`=" . $order_tax_amount_usd . ", 
      `discount_amount_usd`=" . $order_discount_amount_usd . ", 
      `total_usd`=" . $order_total_usd . " 
      WHERE `order_id`=" . $order->id . " AND `customer_email`!=''");
    $bigQuery->runQuery($updateOrder);

    $deleteOrderProducts = $bigQuery->query("DELETE FROM `mojo-dashboard-mysql-gds.shopify_orders_analytics.orders` 
      WHERE `order_id`=" . $order->id . " AND `customer_email`=''");
    $bigQuery->runQuery($deleteOrderProducts);

    $line_items_rows = [];
    foreach ($order->line_items as $key => $product) {
      $order_line_item_price_usd = $product->price;
      if ($order->currency != 'USD' AND isset($fxRates[strtoupper($order->currency) . '_to_USD'])) {
        $order_line_item_price_usd = round($order_line_item_price_usd * $fxRates[strtoupper($order->currency) . '_to_USD'], 2);
      }

      $line_items_rows[] = [
        'data' => [
          'shop' => '' . addslashes(htmlspecialchars($shop)) . '',
          'order_id' => $order->id, 
          'order_name' => '' . addslashes(htmlspecialchars($order->name)) . '', 
          'currency' => '' . addslashes(htmlspecialchars($order->currency)) . '', 
          'jcheckout_id' => '' . addslashes(htmlspecialchars($jcheckout_id)) . '', 
          'line_item_quantity' => $product->quantity,  
          'line_item_price' => $product->price, 
          'line_item_name' => '' . addslashes(htmlspecialchars($product->name)) . '', 
          'line_item_sku' => '' . addslashes(htmlspecialchars($product->sku)) . '', 
          'line_item_id' => $product->id, 
          'line_item_price_usd' => $order_line_item_price_usd, 
          'created_at' => '' . addslashes(htmlspecialchars($created_at)) . '',
        ]
      ];
    }
    if (!empty($line_items_rows)) {
      $table->insertRows($line_items_rows);
    }
  } else {
    $payment_method = end($order->payment_gateway_names);

    $insertOrder = $bigQuery->query("INSERT INTO `mojo-dashboard-mysql-gds.shopify_orders_analytics.orders` 
      (
        `shop`, 
        `order_id`, 
        `order_name`, 
        `customer_email`, 
        `financial_status`, 
        `created_at`, 
        `currency`, 
        `subtotal`, 
        `tax_amount`, 
        `discount_code`, 
        `discount_amount`, 
        `total`, 
        `payment_method`, 
        `jcheckout_id`, 
        `created_hour`,
        `subtotal_usd`, 
        `tax_amount_usd`, 
        `discount_amount_usd`, 
        `total_usd`
      )
      VALUES 
      (
        '" . addslashes(htmlspecialchars($shop)) . "', 
        " . $order->id . ", 
        '" . addslashes(htmlspecialchars($order->name)) . "',
        '" . addslashes(htmlspecialchars($order->contact_email)) . "', 
        '" . addslashes(htmlspecialchars($order->financial_status)) . "', 
        '" . addslashes(htmlspecialchars($created_at)) . "', 
        '" . addslashes(htmlspecialchars($order->currency)) . "', 
        " . $order->subtotal_price . ", 
        " . $order->total_tax . ", 
        '" . addslashes(htmlspecialchars($discount_code)) . "', 
        " . $order->total_discounts . ", 
        " . $order->total_price . ", 
        '" . addslashes(htmlspecialchars($payment_method)) . "', 
        '" . addslashes(htmlspecialchars($jcheckout_id)) . "', 
        " . (int) date('H', strtotime($created_at)) . ", 
        " . $order_subtotal_usd . ", 
        " . $order_tax_amount_usd . ", 
        " . $order_discount_amount_usd . ", 
        " . $order_total_usd . "
      )");
    $bigQuery->runQuery($insertOrder);
    
    $line_items_rows = [];
    foreach ($order->line_items as $key => $product) {
      $order_line_item_price_usd = $product->price;
      if ($order->currency != 'USD' AND isset($fxRates[strtoupper($order->currency) . '_to_USD'])) {
        $order_line_item_price_usd = round($order_line_item_price_usd * $fxRates[strtoupper($order->currency) . '_to_USD'], 2);
      }

      $line_items_rows[] = [
        'data' => [
          'shop' => '' . addslashes(htmlspecialchars($shop)) . '',
          'order_id' => $order->id, 
          'order_name' => '' . addslashes(htmlspecialchars($order->name)) . '', 
          'currency' => '' . addslashes(htmlspecialchars($order->currency)) . '', 
          'jcheckout_id' => '' . addslashes(htmlspecialchars($jcheckout_id)) . '', 
          'line_item_quantity' => $product->quantity,  
          'line_item_price' => $product->price, 
          'line_item_name' => '' . addslashes(htmlspecialchars($product->name)) . '', 
          'line_item_sku' => '' . addslashes(htmlspecialchars($product->sku)) . '', 
          'line_item_id' => $product->id, 
          'line_item_price_usd' => $order_line_item_price_usd, 
          'created_at' => '' . addslashes(htmlspecialchars($created_at)) . '',
        ]
      ];
    }
    if (!empty($line_items_rows)) {
      $table->insertRows($line_items_rows);
    }

    if (!empty($order->shipping_lines)) {
      $shipping_line_rows = [];
      foreach ($order->shipping_lines as $key => $shipping) {
        if ($shipping->price > 0) {
          $order_shipping_method_amount_usd = $shipping->price;
          if ($order->currency != 'USD' AND isset($fxRates[strtoupper($order->currency) . '_to_USD'])) {
            $order_shipping_method_amount_usd = round($order_shipping_method_amount_usd * $fxRates[strtoupper($order->currency) . '_to_USD'], 2);
          }

          $shipping_line_rows[] = [
            'data' => [
              'shop' => '' . addslashes(htmlspecialchars($shop)) . '',
              'order_id' => $order->id, 
              'order_name' => '' . addslashes(htmlspecialchars($order->name)) . '', 
              'currency' => '' . addslashes(htmlspecialchars($order->currency)) . '', 
              'jcheckout_id' => '' . addslashes(htmlspecialchars($jcheckout_id)) . '', 
              'shipping_method' => '' . addslashes(htmlspecialchars($shipping->title)) . '', 
              'shipping_method_amount' => $shipping->price, 
              'shipping_method_amount_usd' => $order_shipping_method_amount_usd, 
              'created_at' => '' . addslashes(htmlspecialchars($created_at)) . '',
            ]
          ];
        }
      }
      
      if (!empty($shipping_line_rows)) {
        $table->insertRows($shipping_line_rows);
      }
    }
  }

  http_response_code(200);
  exit;
}

http_response_code(401);
?>
